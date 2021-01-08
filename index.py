from flask import Flask
from ca_simulation3 import simulation_1day
import datetime

app = Flask(__name__)

@app.route('/simulation', methods=['GET'])
def main():
    
    
    today = datetime.date.today()
    start_date = str(today)
    start_date = '2021-01-07'
    date = start_date
    
    simulation_1day(date)
    return "The simulation is executed successfully on "+ str(start_date) +", Please Check in the ca_simulationdb"
    
@app.route('/test', methods=['GET'])
def test():
    return "Hello World!!!!!!!!!!!"
    



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
    