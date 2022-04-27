import json

# from app import lambda_function

event_file = open('event.json')


def test_changes():
    event = json.load(event_file)
    context = None

    records = event['Records'][0]
    commit = records['codecommit']['references'][0]['commit']

    assert records['eventName'] == "ReferenceChanges",  "the lambda event is invalid"
    assert len(event['Records']) != 0,  "the records list is empty"
    assert len(commit) == 40, "the commit id is invalid"

    # return lambda_function.lambda_handler(event, context)


if __name__ == '__main__':
    test_changes()
