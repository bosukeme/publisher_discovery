from flask_restful import Api
from flask import Flask
from resources import PublisherDiscovery

app = Flask(__name__)

api = Api(app)

@app.route("/")
def home():
    return "<h1 style='color:blue'> This is the Publisher Discovery Script Pipeline </h1>"


api.add_resource(PublisherDiscovery, '/publisher_discovery')

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)