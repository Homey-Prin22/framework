def align_sensors_data(sensors_data: dict) -> list[dict]:
    timestamps = sensors_data.get("timestamp", [])
    if isinstance(timestamps, dict) and "value" in timestamps:
        timestamps = [timestamps["value"]]
    elif isinstance(timestamps, (int, float)):
        timestamps = [timestamps]

    aligned = []

    for i, ts in enumerate(timestamps):
        entry = {"timestamp": ts}
        for sensor, values in sensors_data.items():
            if sensor == "timestamp":
                continue
            if isinstance(values, dict):
                for axis, arr in values.items():
                    if isinstance(arr, list):
                            if i < len(arr):
                                entry[f"{sensor}_{axis}"] = arr[i]
                    else:
                        entry[f"{sensor}_{axis}"] = arr
        aligned.append(entry)
    #print(aligned)
    return aligned

def align_movella_data(movella_data: dict) -> list[dict]:
    timestamps = movella_data.get("timestamp", [])
    if isinstance(timestamps, dict) and "value" in timestamps:
        timestamps = [timestamps["value"]]
    elif isinstance(timestamps, (int, float)):
        timestamps = [timestamps]

    aligned = []

    for i, ts in enumerate(timestamps):
        entry = {"timestamp": ts}
        for dot_key in [k for k in movella_data if k.startswith("DOT")]:
            dot_data = movella_data[dot_key]
            for axis in ["x", "y", "z"]:
                values = dot_data.get(axis, [])
                if isinstance(values, list):
                    if i < len(values):
                        entry[f"{dot_key}_{axis}"] = values[i]
        aligned.append(entry)
    print(aligned)
    return aligned



def align_robot_data(robot_data: dict) -> list[dict]:
    timestamps = robot_data.get("timestamp", [])
    if isinstance(timestamps, dict) and "value" in timestamps:
        timestamps = [timestamps["value"]]
    elif isinstance(timestamps, (int, float)):
        timestamps = [timestamps]

    aligned = []

    for i, ts in enumerate(timestamps):
        entry = {"timestamp": ts}
        for key, values in robot_data.items():
            if key == "timestamp":
                continue
            if isinstance(values, list):
                if i < len(values):
                    entry[key] = values[i]
            elif isinstance(values, (int, float)):  # handle scalar
                entry[key] = values
        aligned.append(entry)
    print(aligned)
    return aligned
