from ftplib import all_errors
import os
import json
import paho.mqtt.subscribe as subscribe
import paho.mqtt.publish as publish
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

def read_lines(file):
    with open(os.path.join(DIR, file), "r+") as f:
        lines = {line.rstrip('\n') for line in f}
    return lines

def write_lines(file, lines):
    with open(os.path.join(DIR, file), "w+") as f:
        for s in sorted(lines):
            f.write(s+"\n")

def check_new_shows():
    """Check for any new folders"""
    # Read in all shows that we've already noticed
    found_shows = read_lines("found.txt")

    # Read in all shows with folders
    all_shows = {f.name for f in os.scandir(DIR) if f.is_dir()}

    # Publish them
    new_shows = all_shows - found_shows
    if len(new_shows) > 0:
        logging.info(f"Found new folders: {new_shows}")
        publish.single(
            TOPIC_FOUND, 
            payload=json.dumps(list(new_shows)),
            qos=1,
            hostname=MQTT_ADDRESS, 
            port=MQTT_PORT,
            auth={"username": MQTT_USER, "password": MQTT_PASS}
        )

        # Save those to file
    if len(new_shows) > 0:
        write_lines("found.txt", all_shows)

    return new_shows

def check_new_clean(are_new_shows):
    """Gets new shows if there is any, otherwise reads in cleaning list"""
    # Get shows in file, and any messages
    to_clean = read_lines("cleaning.txt")

    if are_new_shows:
        logging.info("Waiting to hear if should monitor new shows...")
        new_to_clean = subscribe.simple(
            TOPIC_ADD,
            hostname=MQTT_ADDRESS, 
            port=MQTT_PORT,
            auth={"username": MQTT_USER, "password": MQTT_PASS}
        )
        new_to_clean = set(json.loads(new_to_clean.payload))
        logging.info(f"Now monitoring: {new_to_clean}")
        to_clean |= new_to_clean

        # Rewrite all shows to file
        write_lines("cleaning.txt", to_clean)

    return to_clean

def clean(to_clean):
    """Clean all folders found in the cleaning file"""
    errors = set()
    logging.info("Beginning all cleaning")
    for s in to_clean:
        # Find all files that need to be cleaned
        dir = os.path.join(DIR, s, "**/*")
        files = [f for f in iglob(dir, recursive=True) if os.path.isfile(f) 
                                                        and "_clean" not in f 
                                                        and os.path.splitext(f)[1] != ".srt"]
        files = sorted(files)

        if len(files) > 0:
            logging.info(f"[{s}] Beginning Cleaning")

        # Iterate through them
        for f in files:
            try:
                file_name = os.path.basename(f)
                logging.info(f"[{file_name}] Subtitles")

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
                logging.info(f"[{file_name}] Cleaning Video")
                cleaner.MultiplexCleanVideo()

                # Remove the old files, and extra subtitles
                logging.info(f"[{file_name}] Removing extras")
                os.remove(f)
                os.remove(subs)

            except:
                logging.warning(f"[{file_name}] Failed")
                errors.add(file_name)

    logging.info("Finished cleaning")
    if len(errors) > 0:
        old_errors = read_lines("errors.txt")
        new_errors = errors - old_errors

        if len(new_errors) > 0:
            logging.warning(f"Had errors on files: {new_errors}")
            # Send error messages for HA to follow up on
            publish.single(
                TOPIC_ERROR, 
                payload=json.dumps(list(new_errors)),
                qos=1,
                hostname=MQTT_ADDRESS, 
                port=MQTT_PORT,
                auth={"username": MQTT_USER, "password": MQTT_PASS}
            )
            write_lines("errors.txt", errors|old_errors)
        

def Monitor():
    # Setup logging
    logging.basicConfig(
         format='[%(asctime)s] %(levelname)-4s %(message)s',
         level=logging.INFO,
         datefmt='%Y-%m-%d %H:%M:%S')

    # Turn off a bunch of logging
    logging.getLogger('subliminal.providers.opensubtitles').setLevel(logging.WARNING)
    logging.getLogger('subliminal.providers.podnapisi').setLevel(logging.WARNING)
    logging.getLogger('subliminal.score').setLevel(logging.WARNING)
    logging.getLogger('subliminal.subtitle').setLevel(logging.WARNING)
    logging.getLogger('subliminal.video').setLevel(logging.WARNING)
    logging.getLogger('subliminal').setLevel(logging.WARNING)

    # for key in logging.root.manager.loggerDict:
    #     print(key)
    # return

    # Get started
    while True:
        # Check for any new folders
        new_shows = check_new_shows()        
        # check if there's new shows to be cleaned
        to_clean = check_new_clean(len(new_shows) > 0)
        # Do the cleaning!
        clean(to_clean)
        # wait
        time.sleep(WAIT*60)

if __name__ == "__main__":
    Monitor()
