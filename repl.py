import zmq, time, ujson, pprint

context = zmq.Context()

# Socket to talk to server
print "Connecting to server"
socket = context.socket(zmq.REQ)
socket.connect ("tcp://127.0.0.1:5555") # TODO: parameterize hostname/port

while True:
    try:
        cmd = raw_input("> ").strip()
        if cmd.lower() == "quit":
            break
        if not cmd:
            continue
        socket.send (cmd)
        result = ujson.loads(socket.recv())
        pprint.pprint(result)
    except KeyboardInterrupt:
        print "\ngot interrupt\n"
        pass

