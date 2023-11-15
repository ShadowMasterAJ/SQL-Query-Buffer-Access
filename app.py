from flask import Flask, jsonify, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/visualise_query')
def get_query_data():
    # Generate and return your JSON data here
    #TODO: API FOR PLAN.JSON INTO VUEJS COMPONENT
    return jsonify(query_data)

if __name__ == '__main__':
    app.run(debug=True)
