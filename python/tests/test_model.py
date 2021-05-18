#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `pynessie` package."""

from json import loads

from pynessie.model import BranchSchema, MultiContentsSchema


class TestIgnoreProperties(object):
    """Test whether JSON deserialization works with unknown properties.

    The JSON strings here have been "captures" from the output of the
    `org.projectnessie.model.TestIgnoreProperties` test by uncommenting
    a `System.err.println`.
    """

    def test_branch(self: "TestIgnoreProperties") -> None:
        """Test Branch deserialization."""
        str = u"""
        {
          "someUnknownField_1" : "foo bar",
          "someUnknownField_2" : [ "meep", "meep", "meep" ],
          "someUnknownField_3" : {
            "someUnknownField_4" : "foo bar"
          },
          "type" : "BRANCH",
          "someUnknownField_5" : "foo bar",
          "someUnknownField_6" : [ "meep", "meep", "meep" ],
          "someUnknownField_7" : {
            "someUnknownField_8" : "foo bar"
          },
          "name" : "name",
          "someUnknownField_9" : "foo bar",
          "someUnknownField_10" : [ "meep", "meep", "meep" ],
          "someUnknownField_11" : {
            "someUnknownField_12" : "foo bar"
          },
          "hash" : "1234123412341234123412341234123412341234"
        }
        """
        json = loads(str)
        BranchSchema().load(json)

    def test_tag(self: "TestIgnoreProperties") -> None:
        """Test Tag deserialization."""
        str = u"""
        {
          "someUnknownField_1" : "foo bar",
          "someUnknownField_2" : [ "meep", "meep", "meep" ],
          "someUnknownField_3" : {
            "someUnknownField_4" : "foo bar"
          },
          "type" : "TAG",
          "someUnknownField_5" : "foo bar",
          "someUnknownField_6" : [ "meep", "meep", "meep" ],
          "someUnknownField_7" : {
            "someUnknownField_8" : "foo bar"
          },
          "name" : "name",
          "someUnknownField_9" : "foo bar",
          "someUnknownField_10" : [ "meep", "meep", "meep" ],
          "someUnknownField_11" : {
            "someUnknownField_12" : "foo bar"
          },
          "hash" : "1234123412341234123412341234123412341234"
        }
        """
        json = loads(str)
        BranchSchema().load(json)

    def test_hash(self: "TestIgnoreProperties") -> None:
        """Test Hash deserialization."""
        str = u"""
        {
          "someUnknownField_1" : "foo bar",
          "someUnknownField_2" : [ "meep", "meep", "meep" ],
          "someUnknownField_3" : {
            "someUnknownField_4" : "foo bar"
          },
          "type" : "HASH",
          "someUnknownField_5" : "foo bar",
          "someUnknownField_6" : [ "meep", "meep", "meep" ],
          "someUnknownField_7" : {
            "someUnknownField_8" : "foo bar"
          },
          "name" : "1234123412341234123412341234123412341234",
          "someUnknownField_9" : "foo bar",
          "someUnknownField_10" : [ "meep", "meep", "meep" ],
          "someUnknownField_11" : {
            "someUnknownField_12" : "foo bar"
          },
          "hash" : "1234123412341234123412341234123412341234"
        }
        """
        json = loads(str)
        BranchSchema().load(json)

    def test_operations(self: "TestIgnoreProperties") -> None:
        """Test MultiContentsSchema (= Operations in Java) deserialization."""
        str = u"""
        {
          "someUnknownField_1" : "foo bar",
          "someUnknownField_2" : [ "meep", "meep", "meep" ],
          "someUnknownField_3" : {
            "someUnknownField_4" : "foo bar"
          },
          "commitMeta" : {
            "someUnknownField_5" : "foo bar",
            "someUnknownField_6" : [ "meep", "meep", "meep" ],
            "someUnknownField_7" : {
              "someUnknownField_8" : "foo bar"
            },
            "hash" : "1234123412341234123412341234123412341234",
            "someUnknownField_9" : "foo bar",
            "someUnknownField_10" : [ "meep", "meep", "meep" ],
            "someUnknownField_11" : {
              "someUnknownField_12" : "foo bar"
            },
            "committer" : null,
            "someUnknownField_13" : "foo bar",
            "someUnknownField_14" : [ "meep", "meep", "meep" ],
            "someUnknownField_15" : {
              "someUnknownField_16" : "foo bar"
            },
            "author" : null,
            "someUnknownField_17" : "foo bar",
            "someUnknownField_18" : [ "meep", "meep", "meep" ],
            "someUnknownField_19" : {
              "someUnknownField_20" : "foo bar"
            },
            "signedOffBy" : null,
            "someUnknownField_21" : "foo bar",
            "someUnknownField_22" : [ "meep", "meep", "meep" ],
            "someUnknownField_23" : {
              "someUnknownField_24" : "foo bar"
            },
            "message" : "some message",
            "someUnknownField_25" : "foo bar",
            "someUnknownField_26" : [ "meep", "meep", "meep" ],
            "someUnknownField_27" : {
              "someUnknownField_28" : "foo bar"
            },
            "commitTime" : "2021-05-18T13:15:11.646155Z",
            "someUnknownField_29" : "foo bar",
            "someUnknownField_30" : [ "meep", "meep", "meep" ],
            "someUnknownField_31" : {
              "someUnknownField_32" : "foo bar"
            },
            "authorTime" : "2021-05-18T13:15:11.646164Z",
            "someUnknownField_33" : "foo bar",
            "someUnknownField_34" : [ "meep", "meep", "meep" ],
            "someUnknownField_35" : {
              "someUnknownField_36" : "foo bar"
            },
            "properties" : { }
          },
          "someUnknownField_37" : "foo bar",
          "someUnknownField_38" : [ "meep", "meep", "meep" ],
          "someUnknownField_39" : {
            "someUnknownField_40" : "foo bar"
          },
          "operations" : [ {
            "someUnknownField_41" : "foo bar",
            "someUnknownField_42" : [ "meep", "meep", "meep" ],
            "someUnknownField_43" : {
              "someUnknownField_44" : "foo bar"
            },
            "type" : "DELETE",
            "someUnknownField_45" : "foo bar",
            "someUnknownField_46" : [ "meep", "meep", "meep" ],
            "someUnknownField_47" : {
              "someUnknownField_48" : "foo bar"
            },
            "key" : {
              "someUnknownField_49" : "foo bar",
              "someUnknownField_50" : [ "meep", "meep", "meep" ],
              "someUnknownField_51" : {
                "someUnknownField_52" : "foo bar"
              },
              "elements" : [ "delete", "something" ]
            }
          }, {
            "someUnknownField_53" : "foo bar",
            "someUnknownField_54" : [ "meep", "meep", "meep" ],
            "someUnknownField_55" : {
              "someUnknownField_56" : "foo bar"
            },
            "type" : "UNCHANGED",
            "someUnknownField_57" : "foo bar",
            "someUnknownField_58" : [ "meep", "meep", "meep" ],
            "someUnknownField_59" : {
              "someUnknownField_60" : "foo bar"
            },
            "key" : {
              "someUnknownField_61" : "foo bar",
              "someUnknownField_62" : [ "meep", "meep", "meep" ],
              "someUnknownField_63" : {
                "someUnknownField_64" : "foo bar"
              },
              "elements" : [ "nothing", "changed" ]
            }
          }, {
            "someUnknownField_65" : "foo bar",
            "someUnknownField_66" : [ "meep", "meep", "meep" ],
            "someUnknownField_67" : {
              "someUnknownField_68" : "foo bar"
            },
            "type" : "PUT",
            "someUnknownField_69" : "foo bar",
            "someUnknownField_70" : [ "meep", "meep", "meep" ],
            "someUnknownField_71" : {
              "someUnknownField_72" : "foo bar"
            },
            "key" : {
              "someUnknownField_73" : "foo bar",
              "someUnknownField_74" : [ "meep", "meep", "meep" ],
              "someUnknownField_75" : {
                "someUnknownField_76" : "foo bar"
              },
              "elements" : [ "put", "iceberg" ]
            },
            "someUnknownField_77" : "foo bar",
            "someUnknownField_78" : [ "meep", "meep", "meep" ],
            "someUnknownField_79" : {
              "someUnknownField_80" : "foo bar"
            },
            "contents" : {
              "someUnknownField_81" : "foo bar",
              "someUnknownField_82" : [ "meep", "meep", "meep" ],
              "someUnknownField_83" : {
                "someUnknownField_84" : "foo bar"
              },
              "type" : "ICEBERG_TABLE",
              "someUnknownField_85" : "foo bar",
              "someUnknownField_86" : [ "meep", "meep", "meep" ],
              "someUnknownField_87" : {
                "someUnknownField_88" : "foo bar"
              },
              "id" : "1269b019-ad6f-4be7-8ada-6d1e7ce22770",
              "someUnknownField_89" : "foo bar",
              "someUnknownField_90" : [ "meep", "meep", "meep" ],
              "someUnknownField_91" : {
                "someUnknownField_92" : "foo bar"
              },
              "metadataLocation" : "here/and/there"
            }
          }, {
            "someUnknownField_93" : "foo bar",
            "someUnknownField_94" : [ "meep", "meep", "meep" ],
            "someUnknownField_95" : {
              "someUnknownField_96" : "foo bar"
            },
            "type" : "PUT",
            "someUnknownField_97" : "foo bar",
            "someUnknownField_98" : [ "meep", "meep", "meep" ],
            "someUnknownField_99" : {
              "someUnknownField_100" : "foo bar"
            },
            "key" : {
              "someUnknownField_101" : "foo bar",
              "someUnknownField_102" : [ "meep", "meep", "meep" ],
              "someUnknownField_103" : {
                "someUnknownField_104" : "foo bar"
              },
              "elements" : [ "put", "delta" ]
            },
            "someUnknownField_105" : "foo bar",
            "someUnknownField_106" : [ "meep", "meep", "meep" ],
            "someUnknownField_107" : {
              "someUnknownField_108" : "foo bar"
            },
            "contents" : {
              "someUnknownField_109" : "foo bar",
              "someUnknownField_110" : [ "meep", "meep", "meep" ],
              "someUnknownField_111" : {
                "someUnknownField_112" : "foo bar"
              },
              "type" : "DELTA_LAKE_TABLE",
              "someUnknownField_113" : "foo bar",
              "someUnknownField_114" : [ "meep", "meep", "meep" ],
              "someUnknownField_115" : {
                "someUnknownField_116" : "foo bar"
              },
              "id" : "delta-id",
              "someUnknownField_117" : "foo bar",
              "someUnknownField_118" : [ "meep", "meep", "meep" ],
              "someUnknownField_119" : {
                "someUnknownField_120" : "foo bar"
              },
              "metadataLocationHistory" : [ "meta", "data", "location" ],
              "someUnknownField_121" : "foo bar",
              "someUnknownField_122" : [ "meep", "meep", "meep" ],
              "someUnknownField_123" : {
                "someUnknownField_124" : "foo bar"
              },
              "checkpointLocationHistory" : [ "checkpoint" ],
              "someUnknownField_125" : "foo bar",
              "someUnknownField_126" : [ "meep", "meep", "meep" ],
              "someUnknownField_127" : {
                "someUnknownField_128" : "foo bar"
              },
              "lastCheckpoint" : "checkpoint"
            }
          }, {
            "someUnknownField_129" : "foo bar",
            "someUnknownField_130" : [ "meep", "meep", "meep" ],
            "someUnknownField_131" : {
              "someUnknownField_132" : "foo bar"
            },
            "type" : "PUT",
            "someUnknownField_133" : "foo bar",
            "someUnknownField_134" : [ "meep", "meep", "meep" ],
            "someUnknownField_135" : {
              "someUnknownField_136" : "foo bar"
            },
            "key" : {
              "someUnknownField_137" : "foo bar",
              "someUnknownField_138" : [ "meep", "meep", "meep" ],
              "someUnknownField_139" : {
                "someUnknownField_140" : "foo bar"
              },
              "elements" : [ "put", "hive" ]
            },
            "someUnknownField_141" : "foo bar",
            "someUnknownField_142" : [ "meep", "meep", "meep" ],
            "someUnknownField_143" : {
              "someUnknownField_144" : "foo bar"
            },
            "contents" : {
              "someUnknownField_145" : "foo bar",
              "someUnknownField_146" : [ "meep", "meep", "meep" ],
              "someUnknownField_147" : {
                "someUnknownField_148" : "foo bar"
              },
              "type" : "HIVE_TABLE",
              "someUnknownField_149" : "foo bar",
              "someUnknownField_150" : [ "meep", "meep", "meep" ],
              "someUnknownField_151" : {
                "someUnknownField_152" : "foo bar"
              },
              "id" : "hive-id",
              "someUnknownField_153" : "foo bar",
              "someUnknownField_154" : [ "meep", "meep", "meep" ],
              "someUnknownField_155" : {
                "someUnknownField_156" : "foo bar"
              },
              "tableDefinition" : "AAAAAAAAAAAAAA==",
              "someUnknownField_157" : "foo bar",
              "someUnknownField_158" : [ "meep", "meep", "meep" ],
              "someUnknownField_159" : {
                "someUnknownField_160" : "foo bar"
              },
              "partitions" : [ "AAAAAAA=" ]
            }
          } ]
        }
        """
        json = loads(str)
        MultiContentsSchema().load(json)
