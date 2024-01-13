import pytest
from discoverx.explorer import InfoFetcher, TagsInfo, TagInfo


def test_validate_from_components():
    info_table = TagsInfo([], [TagInfo("a", "v1")], [], [])
    info_schema = TagsInfo([], [], [TagInfo("a", "v1")], [])
    info_catalog = TagsInfo([], [], [], [TagInfo("a", "v1")])
    info_no_tags = TagsInfo([], [], [], [])

    assert InfoFetcher._contains_all_tags(info_table, [TagInfo("a", "v1")])
    assert not InfoFetcher._contains_all_tags(info_table, [TagInfo("a", "v2")])
    assert not InfoFetcher._contains_all_tags(info_table, [TagInfo("b", "v1")])
    assert not InfoFetcher._contains_all_tags(info_table, [TagInfo("a", None)])
    # If no tags to check, then it should be true
    assert InfoFetcher._contains_all_tags(info_table, [])

    assert InfoFetcher._contains_all_tags(info_schema, [TagInfo("a", "v1")])

    assert InfoFetcher._contains_all_tags(info_catalog, [TagInfo("a", "v1")])

    assert InfoFetcher._contains_all_tags(info_no_tags, [])
    assert not InfoFetcher._contains_all_tags(info_no_tags, [TagInfo("a", "v1")])
