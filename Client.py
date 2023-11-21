import zmq
import json
import time

print("Connecting to ES_microservice server...\n")

# Required to connect to server
context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://localhost:5555")
# ************


def send_request(request):
    print(f"Sending request: {request}…")

    # Send request as below
    socket.send(json.dumps(request).encode("utf-8"))
    # ************

    time.sleep(5)

    # Get response back as below
    message = json.loads(socket.recv().decode("utf-8"))
    # ************

    print(f"Received reply for request: {request}\nReply: {message}\n")


# User presses random button
send_request('Random')
time.sleep(10)
# User presses search button and inputs valid pl_name
send_request('11 Com b')
time.sleep(10)
# User presses search button and inputs invalid pl_name
send_request('bebe4')
