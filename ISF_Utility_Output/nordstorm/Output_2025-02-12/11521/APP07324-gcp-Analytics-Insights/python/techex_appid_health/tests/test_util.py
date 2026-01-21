from src.util import clean_string


def test_clean_string() -> None:
    string_list = [
        ("'test'", " test "),
        ('"test"', " test "),
        ("one\ntwo", "one two"),
        ("three\tfour", "three four"),
        ("'test'\none", " test  one"),
    ]

    for s in string_list:
        assert clean_string(s[0]) == s[1]
