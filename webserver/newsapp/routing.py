from channels.routing import route
from newsapp.consumers import ws_message

channel_routing = [
	route("websocket.receive", ws_message),
]
