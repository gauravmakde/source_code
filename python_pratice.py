

def json_Date(json):

    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': json
    }

    return data
print(json_Date("gaurav").get("age"))