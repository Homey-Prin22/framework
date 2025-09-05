from aligners import align_sensors_data, align_movella_data, align_robot_data

def align_payload_by_type(payload: dict) -> list[dict]:
    #print('sei in aligned payload')
    if "sensorData" in payload:
        return align_sensors_data(payload["sensorData"])
    elif "MovellaData" in payload:
        return align_movella_data(payload["MovellaData"])
    elif "gestureData" in payload:
        gesture = payload["gestureData"]
        return [{
            "timestamp": gesture.get("timestampGestureData"),
            "gesture": gesture.get("gesture")
        }]
    elif "RobotData" in payload:
        return align_robot_data(payload["RobotData"])
    else:
        raise ValueError("Unsupported payload structure")
