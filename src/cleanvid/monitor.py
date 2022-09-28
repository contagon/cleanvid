import os
import json
import paho.mqtt.client as paho
import time
from glob import iglob
from cleanvid import VidCleaner, GetSubtitles
import logging

MQTT_ADDRESS = os.environ.get("MQTT_ADDRESS", "localhost")
MQTT_PORT = os.environ.get("MQTT_PORT", 1883)
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")

MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "cleanvid")
TOPIC_FOUND = os.path.join(MQTT_TOPIC, "found")
TOPIC_ADD = os.path.join(MQTT_TOPIC, "to_clean")
TOPIC_ERROR = os.path.join(MQTT_TOPIC, "error")

DIR = os.environ.get("DIRECTORY", "/data")
WAIT = float(os.environ.get("WAIT", 60))

def connect_mqtt_client():
    """CONNECT TO MQTT Broker """
    client = paho.Client("clean_shows")
    client.username_pw_set(MQTT_USER, password=MQTT_PASS)
    client.connect(MQTT_ADDRESS, MQTT_PORT)

    return client

def wait_msg(client, topic):
    """Get message from MQTT"""
    # Listen for message
    global data
    data = []
    def on_message(client, userdata, msg):
        global data
        data = json.loads(msg.payload)

    # subscribe to topic
    client.on_message = on_message
    client.subscribe(topic)

    # Wait till we have a response
    while len(data) == 0:
        client.loop()
        time.sleep(0.1)

    client.unsubscribe(topic)

    return data

def check_new_shows(client):
    """Check for any new folders"""
    # Read in all shows that we've already noticed
    with open(os.path.join(DIR, "found.txt")) as f:
        found_shows = {line.rstrip('\n') for line in f}

    # Read in all shows with folders
    all_shows = {f.name for f in os.scandir(DIR) if f.is_dir()}

    # Save those to file
    if len(all_shows) > 0:
        with open(os.path.join(DIR, "found.txt"), "w") as f:
            for s in sorted(all_shows):
                f.write(s+"\n")

    # Publish them
    new_shows = all_shows - found_shows
    if len(new_shows) > 0:
        logging.info(f"Found new folders: {new_shows}")
        client.publish(TOPIC_FOUND, json.dumps(list(new_shows)))

    return new_shows

def check_new_clean(client, are_new_shows):
    """Gets new shows if there is any, otherwise reads in cleaning list"""
    # Get shows in file, and any messages
    with open(os.path.join(DIR, "cleaning.txt")) as f:
        to_clean = {line.rstrip('\n') for line in f}

    if are_new_shows:
        logging.info("Waiting to hear if should monitor new shows...")
        new_to_clean = set(wait_msg(client, TOPIC_ADD))
        logging.info(f"Now monitoring: {new_to_clean}")
        to_clean |= new_to_clean

    # Rewrite all shows to file
    with open(os.path.join(DIR, "cleaning.txt"), "w") as f:
        for s in sorted(to_clean):
            f.write(s+"\n")

    return to_clean

def clean(client, to_clean):
    """Clean all folders found in the cleaning file"""
    errors = []
    for s in to_clean:
        # Find all files that need to be cleaned
        dir = os.path.join(DIR, s, "**/*")
        files = [f for f in iglob(dir, recursive=True) if os.path.isfile(f) and "_clean" not in f]
        files = sorted(files)

        if len(files) > 0:
            logging.info(f"Cleaning {s}...")

        # Iterate through them
        for f in files:
            try:
                file_name = os.path.basename(f)
                logging.info(f"Starting {file_name}")

                # get subtitle and output files
                subs = GetSubtitles(f, 'eng', False)
                f_parts = os.path.splitext(f)
                out = f_parts[0] + "_clean" + f_parts[1]

                # Clean video!
                cleaner = VidCleaner(
                    iVidFileSpec=f,
                    iSubsFileSpec=subs,
                    oVidFileSpec=out,
                    oSubsFileSpec=None,
                    iSwearsFileSpec=os.path.join(DIR, "swears.txt"),
                    embedSubs=True,
                    fullSubs=True,
                )
                cleaner.CreateCleanSubAndMuteList()
                cleaner.MultiplexCleanVideo()

                # Remove the old files, and extra subtitles
                logging.info(f"Removing {file_name} and it's files...")
                os.remove(f)
                os.remove(subs)

            except:
                logging.warning(f"{file_name} failed")
                errors.append(file_name)

    # Send error messages for HA to follow up on
    client.publish(TOPIC_ERROR, json.dumps(list(errors)))

def Monitor():
    # Setup logging
    logging.basicConfig(
         format='[%(asctime)s] %(levelname)-4s %(message)s',
         level=logging.INFO,
         datefmt='%Y-%m-%d %H:%M:%S')

    # Get started
    logging.info("Connecting to MQTT...")
    client = connect_mqtt_client()
    logging.info("Connected")
    while True:
        # Check for any new folders
        new_shows = check_new_shows(client)        
        # check if there's new shows to be cleaned
        to_clean = check_new_clean(client, len(new_shows) > 0)
        # Do the cleaning!
        clean(client, to_clean)
        # wait
        time.sleep(WAIT*60)

if __name__ == "__main__":
    Monitor()
