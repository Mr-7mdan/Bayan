from asgiref.wsgi import AsgiToWsgi

# Import the FastAPI ASGI app
from app.main import app as asgi_app

# Expose a WSGI application for Waitress
application = AsgiToWsgi(asgi_app)
