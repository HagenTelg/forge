import pytest
import typing
from forge.cli.commands.parse import split_tagged_regex


def test_split_tagged_regex():
    assert split_tagged_regex("") == [("", "")]

    assert split_tagged_regex("notag") == [("", "notag")]
    assert split_tagged_regex("no[tag]") == [("", "no[tag]")]
    assert split_tagged_regex("no[ta]g") == [("", "no[ta]g")]
    assert split_tagged_regex("[no]tag") == [("", "[no]tag")]
    assert split_tagged_regex("notag1,notag2") == [("", "notag1"), ("", "notag2")]
    assert split_tagged_regex("notag1,notag2,notag3") == [("", "notag1"), ("", "notag2"), ("", "notag3")]
    assert split_tagged_regex("tag1:value1") == [("tag1", "value1")]
    assert split_tagged_regex("tag1:value1,tag2:value2") == [("tag1", "value1"), ("tag2", "value2")]
    assert split_tagged_regex("tag1:value1,tag2:value2,tag3:value3") == [("tag1", "value1"), ("tag2", "value2"), ("tag3", "value3")]
    assert split_tagged_regex("notag1,tag2:value2") == [("", "notag1"), ("tag2", "value2")]
    assert split_tagged_regex("tag1:value1,notag2") == [("tag1", "value1"), ("", "notag2")]
    assert split_tagged_regex("tag1:value1:extra") == [("tag1", "value1:extra")]
    assert split_tagged_regex("tag1:value1:extra,notag2") == [("tag1", "value1:extra"), ("", "notag2")]
    assert split_tagged_regex("empty:") == [("empty", "")]

    assert split_tagged_regex("(re:pattern)") == [("", "(re:pattern)")]
    assert split_tagged_regex("notag1,(re:pattern)") == [("", "notag1"), ("", "(re:pattern)")]
    assert split_tagged_regex("(re:pattern),notag2") == [("", "(re:pattern)"), ("", "notag2")]
    assert split_tagged_regex("tag1:value1,(re:pattern)") == [("tag1", "value1"), ("", "(re:pattern)")]
    assert split_tagged_regex("(re:pattern),tag2:value2") == [("", "(re:pattern)"), ("tag2", "value2")]
    assert split_tagged_regex("retag:(re:pattern)") == [("retag", "(re:pattern)")]
    assert split_tagged_regex("retag1:(re1:pattern), retag2:(re2:pattern)") == [("retag1", "(re1:pattern)"), ("retag2", "(re2:pattern)")]

    assert split_tagged_regex("empty1:,empty2:") == [("empty1", ""), ("empty2", "")]
    assert split_tagged_regex("empty1:,tag1:value1") == [("empty1", ""), ("tag1", "value1")]
    assert split_tagged_regex("tag1:value1,empty1:") == [("tag1", "value1"), ("empty1", "")]

    assert split_tagged_regex("vc;vc") == [("", "vc;vc"), ]
    assert split_tagged_regex("tag:vc;vc") == [("tag", "vc;vc"), ]
