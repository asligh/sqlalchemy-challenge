import numpy as np

import sqlalchemy
import datetime as dt

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurements = Base.classes.measurement
Stations     = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    q = session.query(Measurements.date).order_by(Measurements.date.desc()).first()
    end_date_obj   = dt.datetime.strptime(q.date,'%Y-%m-%d')
    start_date_obj  = dt.datetime.strptime(str(end_date_obj - dt.timedelta(days=365)), '%Y-%m-%d %H:%M:%S').date()

    sel = [Measurements.date,Measurements.prcp]

    results = session.query(*sel).filter(Measurements.date >= start_date_obj).all()

    results_dict = {}

    for date, prcp in results:

        measurement_date = date
        measurement_prcp = prcp     

        results_dict["date"] = measurement_date
        results_dict["prcp"] = measurement_prcp
        results_dict.update({measurement_date:measurement_prcp})

    session.close()

    return jsonify(results_dict)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations"""
    results = session.query(Stations.name).all()

    session.close()

    # Convert list of tuples into normal list
    station_names = list(np.ravel(results))

    return jsonify(station_names)


@app.route("/api/v1.0/tobs")
def tobs():
     # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of tobs"""

    records = session.query(Stations.id,Stations.station,func.count(Stations.id)).\
                      filter(Measurements.station == Stations.station).\
                      group_by(Stations.id,Stations.name).\
                      order_by(func.count(Stations.id).desc()).\
                      all()

    most_active_station = records[0][1]

    end_date = session.query(Measurements.date).\
                       filter(Measurements.station == Stations.station).\
                       filter(Measurements.station == most_active_station).\
                       order_by(Measurements.date.desc()).\
                       first()

    end_date_obj   = dt.datetime.strptime(end_date.date,'%Y-%m-%d')
    start_date_obj = dt.datetime.strptime(str(end_date_obj - dt.timedelta(days=365)), '%Y-%m-%d %H:%M:%S').date()

    records = session.query(Measurements.date, Stations.station, Measurements.tobs).\
                      filter(Measurements.station == Stations.station).\
                      filter(Measurements.station == most_active_station).\
                      filter(Measurements.date >= start_date_obj).\
                      order_by(Measurements.date).\
                      all()

    session.close()

    all_tobs = []

    for date, station, tobs in records:
         tobs_dict = {}
         tobs_dict["date"]    = date
         tobs_dict["station"] = station
         tobs_dict["tobs"]     = tobs
         all_tobs.append(tobs_dict)

    return jsonify(all_tobs)

@app.route("/api/v1.0/<start>")
def get_tobs_by_start_date(start):
     
      # Create our session (link) from Python to the DB
    session = Session(engine)
     
    start_date_obj = dt.datetime.strptime(start, '%Y-%m-%d')

    records = session.query(func.min(Measurements.tobs), func.avg(Measurements.tobs), func.max(Measurements.tobs)).\
            filter(Measurements.station == Stations.station).\
            filter(Measurements.date >= start_date_obj).\
            order_by(Measurements.date).\
            all()

    rec_list = list(np.ravel(records))

    session.close()    

    return jsonify(rec_list)
    
@app.route("/api/v1.0/<start>/<end>")
def get_tobs_by_start_end_date(start,end):
     
      # Create our session (link) from Python to the DB
    session = Session(engine)
     
    start_date_obj = dt.datetime.strptime(start, '%Y-%m-%d')
    end_date_obj   = dt.datetime.strptime(end, '%Y-%m-%d')   

    records = session.query(func.min(Measurements.tobs), func.avg(Measurements.tobs), func.max(Measurements.tobs)).\
            filter(Measurements.station == Stations.station).\
            filter(Measurements.date >= start_date_obj).\
            filter(Measurements.date <= end_date_obj).\
            order_by(Measurements.date).\
            all()

    rec_list = list(np.ravel(records))

    session.close()    

    return jsonify(rec_list)

if __name__ == '__main__':
    app.run(debug=True)