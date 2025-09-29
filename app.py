import os
import boto3
import base64
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler
from flask import Flask, request, jsonify
from datetime import datetime, timezone, timedelta
from PIL import Image 
import io 

app = Flask(__name__)
if not os.path.exists('logs'):
    os.makedirs('logs')
handler = TimedRotatingFileHandler('logs/app.log', when='midnight', interval=1, backupCount=7, encoding='utf-8')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

PROCESSED_TRACKERS = {}
CACHE_EXPIRY_SECONDS = 3600 # in seconds

REKOGNITION_ACCESS_KEY_ID = os.environ.get('REKOGNITION_ACCESS_KEY_ID')
REKOGNITION_SECRET_ACCESS_KEY = os.environ.get('REKOGNITION_SECRET_ACCESS_KEY')
DYNAMODB_ACCESS_KEY_ID = os.environ.get('DYNAMODB_ACCESS_KEY_ID')
DYNAMODB_SECRET_ACCESS_KEY = os.environ.get('DYNAMODB_SECRET_ACCESS_KEY')

DYNAMODB_TABLE_NAME = 'AIOS-AQ-18'

AWS_REGION = 'us-east-1'
LOCATION_ID = 'CAM05'

try:
    rekognition_client = boto3.client('rekognition',
                                      aws_access_key_id=REKOGNITION_ACCESS_KEY_ID,
                                      aws_secret_access_key=REKOGNITION_SECRET_ACCESS_KEY,
                                      region_name=AWS_REGION)
    dynamodb_resource = boto3.resource('dynamodb',
                                       aws_access_key_id=DYNAMODB_ACCESS_KEY_ID,
                                       aws_secret_access_key=DYNAMODB_SECRET_ACCESS_KEY,
                                       region_name=AWS_REGION)
    analytics_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
    app.logger.info("AWS clients initialized successfully.")
except Exception as e:
    app.logger.critical("Failed to initialize AWS clients: %s", e, exc_info=True)
    rekognition_client = None
    analytics_table = None

def _extract_image_b64(messages):
    """Finds the Base64 image string in the messages payload."""
    for key, value in messages.items():
        if key.startswith("RTSPStreamReceiver_") and isinstance(value, dict):
            return value.get("data")
    return None

def _extract_person_detections(messages):
    """Finds the list of person detection metadata in the payload."""
    detections = []
    for key, value in messages.items():
        if key.startswith("PolylineWithDirection_") and isinstance(value, dict):
            data_list = value.get("data") or []
            for item in data_list:
                if item and item.get("label") == "Person":
                    detections.append(item)
    return detections

def get_age_bracket(age):
    """
    Maps a given age to a predefined bracket string.
    """
    if age <= 12: return "age_0_12"
    elif age <= 18: return "age_12_18"
    elif age <= 24: return "age_18_24"
    elif age <= 36: return "age_24_36"
    else: return "age_36_plus"

@app.route('/analyze', methods=['POST'])
def analyze_frame():
    try:
        payload = request.get_json(force=True, silent=True) or {}
    except Exception as e:
        app.logger.warning("Received invalid JSON: %s", e)
        return jsonify({"error": "Invalid JSON"}), 400

    messages = payload.get("messages", {})
    if not isinstance(messages, dict) or not messages:
        return jsonify({"error": "Payload must contain 'messages' object"}), 400

    frame_b64 = _extract_image_b64(messages)
    if not frame_b64:
        return jsonify({"ok": True, "reason": "no_image_in_payload"}), 200

    person_detections = _extract_person_detections(messages)
    if not person_detections:
        return jsonify({"ok": True, "reason": "no_person_detections_in_payload"}), 200

    try:
        full_image_bytes = base64.b64decode(frame_b64)
        full_frame_image = Image.open(io.BytesIO(full_image_bytes))
        img_width, img_height = full_frame_image.size
    except Exception as e:
        app.logger.error("Failed to decode or open Base64 image: %s", e)
        return jsonify({"error": "Invalid or corrupt image data"}), 400

    processed_count = 0
    unique_new_detections = 0
    utc_now = datetime.now(timezone.utc)
    partition_key = f"{LOCATION_ID}#{utc_now.strftime('%Y-%m-%d')}"
    sort_key = utc_now.hour

    expiry_time = utc_now - timedelta(seconds=CACHE_EXPIRY_SECONDS)
    expired_ids = [tid for tid, ts in PROCESSED_TRACKERS.items() if ts < expiry_time]
    for tid in expired_ids:
        del PROCESSED_TRACKERS[tid]

    for detection in person_detections:
        try:
            tracker_id = detection.get('tracker_id')
            if not tracker_id:
                app.logger.warning("Skipping detection with no tracker_id.")
                continue

            if tracker_id in PROCESSED_TRACKERS:
                app.logger.info("Skipping already processed tracker_id: %s", tracker_id)
                continue

            unique_new_detections += 1
            bbox = detection.get('boundingBoxHistory', [{}])[-1]
            x, y, w, h = bbox.get('x'), bbox.get('y'), bbox.get('width'), bbox.get('height')

            if not all(isinstance(i, (int, float)) for i in [x, y, w, h]):
                app.logger.warning("Skipping detection with invalid bounding box for tracker_id: %s", tracker_id)
                continue

            left = int(x * img_width)
            top = int(y * img_height)
            right = int((x + w) * img_width)
            bottom = int((y + h) * img_height)
            person_crop_img = full_frame_image.crop((left, top, right, bottom))
            
            with io.BytesIO() as output:
                person_crop_img.save(output, format="JPEG")
                crop_bytes = output.getvalue()

            response = rekognition_client.detect_faces(Image={'Bytes': crop_bytes},
                                                     Attributes=['AGE_RANGE', 'GENDER'])
            
            if not response.get('FaceDetails'):
                app.logger.info("Processed a crop for tracker_id %s, but Rekognition found no face in it.", tracker_id)
                continue

            face = response['FaceDetails'][0]
            
            gender = "women" if face['Gender']['Value'] == 'Female' else "men"
            avg_age = (face['AgeRange']['Low'] + face['AgeRange']['High']) / 2
            age_bracket = get_age_bracket(avg_age)
            age_gender_attribute = f"{gender}_{age_bracket}"

            analytics_table.update_item(
                Key={'LocationDate': partition_key, 'Hour': sort_key},
                UpdateExpression="ADD #total_count :val, #total_gender :val, #age_gender_attr :val",
                ExpressionAttributeNames={
                    '#total_count': 'total_count',
                    '#total_gender': f'total_{gender}',
                    '#age_gender_attr': age_gender_attribute
                },
                ExpressionAttributeValues={':val': 1}
            )
            processed_count += 1
            PROCESSED_TRACKERS[tracker_id] = utc_now

        except Exception as e:
            app.logger.error("Failed to process one person detection: %s", e, exc_info=True)
            continue 
            
    app.logger.info(
        "Processed payload. Detections in payload: %d, Unique new detections processed: %d, DB updates sent: %d",
        len(person_detections), unique_new_detections, processed_count
    )

    return jsonify({
        "ok": True,
        "detections_in_payload": len(person_detections),
        "unique_new_detections": unique_new_detections,
        "metrics_updated_count": processed_count
    }), 200

if __name__ == '__main__':
    app.logger.info("Flask server starting up...")
    app.run(host='0.0.0.0', port=5151, debug=False)

