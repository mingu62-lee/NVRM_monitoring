import subprocess
from datetime import datetime
from kafka import KafkaProducer

BROKER = 'ip:9092'
TOPIC = 'gpu-status'

HOST = subprocess.getoutput("hostname")

producer = KafkaProducer(
    bootstrap_servers=[f'{BROKER}:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

proc = subprocess.Popen(
    ["journalctl", "-f", "-k"],
    stdout = subprocess.PIPE,
    stderr = subprocess.PIPE,
    text = True
)

def check_sssd():
    try:
        result = subprocess.check_output({"systemctl", "is-activate", "sssd"}, text=True).strip()
        return result != "activate"
    except:
        return True

def sssd_msg():
    if check_sssd():
        return "check sssd"
    else:
        return "ok"

def main():
    for line in proc.stdout:
        if any(x in line for x in ["NVRM", "Xid", "GPU has fallen", "rm: fatal", "Uncoreectable ECC"]):
            ts = datetime.now().isoformat(timespec='seconds')
            smsg = sssd_msg()
            msg = f"{HOST}|{ts}|{line.strip()}|{smsg}"
            producer.send(TOPIC, msg)

if __name__ == "__main__":
    main()
