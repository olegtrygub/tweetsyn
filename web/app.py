import flask
import redis
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = flask.Flask(__name__)
red = redis.StrictRedis(host='', port=6379, db=0)
producer = KafkaProducer(bootstrap_servers=[''])

pubsub = red.pubsub()
pubsub.subscribe("testtopic")
@app.route('/')
def my_form():
    return flask.render_template("index.html")

@app.route('/subscribe', methods=['POST'])
def post():
        message = flask.request.form['query-subscribe']
        pubsub.subscribe(message)
        producer.send('queriesStream', bytes(message + ':' + message))
        return flask.Response(status=204)

@app.route('/unsubscribe', methods=['POST'])
def post():
        message = flask.request.form['query-unsubscribe']
        pubsub.un(message)
        return flask.Response(status=204)


@app.route('/stream')
def stream():
    return flask.Response(evstream(), mimetype="text/event-stream")

def evstream():
        i = 0
        while i < 100:
                message = pubsub.get_message()
                if message:
                        yield 'data: %s\n\n' % message['data']
                i = i + 1

if __name__ == '__main__':
        app.run(host="0.0.0.0", port=5000, threaded = True)
