import os
import uuid
import json
import sqlite3
from flask import Flask, redirect, render_template_string, request, send_from_directory, jsonify
from confluent_kafka import Producer, KafkaError

# Kafka configuration
KAFKA_CONF = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'client.id': 'flask_app'
}

# Kafka Producer setup
producer = Producer(KAFKA_CONF)

IMAGES_DIR = "images"
MAIN_DB = "main.db"

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    return conn

def produce_message(topic, key, value):
    try:
        producer.produce(topic, key=key, value=value)
        producer.flush()
    except KafkaError as e:
        print(f"Failed to produce message: {e}")

def init_db():
    with get_db_connection() as con:
        con.execute("CREATE TABLE IF NOT EXISTS image(id TEXT PRIMARY KEY, filename TEXT, object TEXT)")

if not os.path.exists(IMAGES_DIR):
    os.mkdir(IMAGES_DIR)

init_db()

@app.route('/', methods=['GET'])
def index():
    with get_db_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT * FROM image")
        images = cur.fetchall()
    return render_template_string("""
<!DOCTYPE html>
<html>
<head>
<style>
.container {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  grid-auto-rows: minmax(100px, auto);
  gap: 20px;
}
img {
   display: block;
   max-width:100%;
   max-height:100%;
   margin-left: auto;
   margin-right: auto;
}
.img {
   height: 270px;
}
.label {
   height: 30px;
  text-align: center;
}
</style>
</head>
<body>
<form method="post" enctype="multipart/form-data">
  <div>
    <label for="file">Choose file to upload</label>
    <input type="file" id="file" name="file" accept="image/x-png,image/gif,image/jpeg" />
  </div>
  <div>
    <button>Submit</button>
  </div>
</form>
<div class="container">
{% for image in images %}
<div>
<div class="img"><img src="/images/{{ image.filename }}"></div>
<div class="label">{{ image.object  }}</div>
</div>
{% endfor %}
</div>
</body>
</html>
    """, images=images)

@app.route('/images/<path:path>', methods=['GET'])
def image(path):
    return send_from_directory(IMAGES_DIR, path)

@app.route('/object/<id>', methods=['PUT'])
def set_object(id):
    json_data = request.json
    obj = json_data.get('object', '')
    with get_db_connection() as con:
        cur = con.cursor()
        cur.execute("UPDATE image SET object = ? WHERE id = ?", (obj, id))
        con.commit()
    return jsonify({"status": "OK"})

@app.route('/', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect('/')
    
    f = request.files['file']
    if f.filename == '':
        return redirect('/')
    
    ext = f.filename.rsplit('.', 1)[-1].lower()
    id = uuid.uuid4().hex
    filename = f"{id}.{ext}"
    file_path = os.path.join(IMAGES_DIR, filename)
    
    try:
        f.save(file_path)
        # Produce the metadata to Kafka
        produce_message(me, None, json.dumps({"id": id, "filename": filename}))
        # Update the database
        with get_db_connection() as con:
            cur = con.cursor()
            cur.execute("INSERT INTO image (id, filename, object) VALUES (?, ?, ?)", (id, filename, ""))
            con.commit()
    except Exception as e:
        print(f"Error uploading file: {e}")
        return jsonify({"error": "File upload failed"}), 500

    return redirect('/')

if __name__ == "__main__":
    app.run(debug=True)
