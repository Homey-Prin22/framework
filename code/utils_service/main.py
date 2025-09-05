import json
from flask import Flask, request, Response, jsonify
import utils



        
if __name__ == '__main__':
	sensors = utils.get_Sensors_by_username("john.smith")
	print(sensors)
