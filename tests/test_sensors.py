from dhub.sensors.filesystem import FilesystemSensor, EventsToWorkflows


def test_sensor_filesystem(tmp_path):
    workflows = []

    sensors_daemon = FilesystemSensor(tmp_path, workflows)
    assert isinstance(sensors_daemon, FilesystemSensor)
    assert hasattr(sensors_daemon, "start")

    eventworkflows = EventsToWorkflows(tmp_path, workflows)
    assert isinstance(eventworkflows, EventsToWorkflows)
    assert hasattr(eventworkflows, "handle_file")
