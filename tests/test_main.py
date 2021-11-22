import pytest
import src.main as sut
import json 

def _load_valid_event():
    with open('tests/resources/example-event.json', 'r') as f:
        return json.load(f)

def test__is_new_file_event():
    valid_event = _load_valid_event()
    
    assert sut._is_new_file_event(valid_event)
    assert sut._is_new_file_event({}) is False
    assert sut._is_new_file_event({"some": {}}) is False

def test__extract_file_bucket_and_path():
    valid_event = _load_valid_event()
    
    bucket, path = sut._extract_file_bucket_and_path(valid_event)
    assert bucket == "someBucket"
    assert path == "somePath"