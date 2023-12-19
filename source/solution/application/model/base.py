"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import abc
import json
import operator
import typing
from collections import defaultdict
from dataclasses import dataclass, field, fields
from functools import reduce
from typing import Any, ClassVar


class UpdateExpressionParameters(typing.TypedDict):
    Key: dict[str, Any]
    UpdateExpression: str
    ExpressionAttributeValues: dict[str, Any]


@dataclass(frozen=True)
class _Metadata:
    path: list[str]
    composite_index: int | None = None
    view_parts: list[str] | None = None
    marshal_as: type | None = None


M = typing.TypeVar("M", bound="Model")


@dataclass
class Model(abc.ABC):
    """
    Serves as a base model that provides the ability to define fields with specified
    paths for parsing raw data into a representative model as well as marshalling the
    model back to raw data. This is a similar pattern to how struct tags are used in Go.

    When defining fields, a path is supplied as a list that dictates how to extract
    the field from a nested structure. For example, to parse the field "C", it would
    look like:

    Source data:
        {
            "A": {
                "B": {
                    "C": 123
                }
            }
        }

    Field definition:
        c: int = Model.field(["A", "B", "C"])

    When parsing, additional kwargs can be passed straight through to the child class
    if need be. There are also additional parameters for specifying optional and composite
    fields.

    For marshalling, views can be defined that can compose the attributes into the resulting
    data structure.

    All inheriting classes also need to decorated with dataclasses.dataclass
    """

    _metadata_key: ClassVar[str] = "base.Model"
    composite_key_delimiter: ClassVar[str] = "|"

    @classmethod
    def field(
        cls,
        path: str | list[str],
        composite_index: int | None = None,
        optional: bool = False,
        default: Any | None = None,
        default_factory: Any | None = None,
        marshal_as: type | None = None,
    ) -> Any:
        """
        Defines a field that can be parsed based on its path. If composite_index is specified,
        then the field will be parsed based on the path and the index when the value is split
        using the composite_key_delimiter (default '|').

        Setting optional to True will give the field a default value of None, and excluding it
        when marshalling if it has not had a value set.
        """
        if isinstance(path, str):
            path = [path]
        extra: dict[str, Any] = {}
        if optional:
            extra["default"] = None
        elif default is not None:
            extra["default"] = default
        if default_factory is not None:
            extra["default_factory"] = default_factory
        metadata = _Metadata(
            path=path, composite_index=composite_index, marshal_as=marshal_as
        )
        return field(kw_only=True, metadata={cls._metadata_key: metadata}, **extra)

    @classmethod
    def view(
        cls,
        path: str | list[str],
        parts: str | list[str],
        marshal_as: type | None = None,
    ) -> Any:
        """
        Defines a view used when marshalling the model. A view defines how the data will be marshalled
        to a specified path and composed by joining the parts using the composite_key_delimiter.

        Parts must be a string representation of the attribute names to compose the data from.
        """
        if isinstance(path, str):
            path = [path]
        if isinstance(parts, str):
            parts = [parts]
        metadata = _Metadata(path=path, view_parts=parts, marshal_as=marshal_as)
        return field(
            metadata={cls._metadata_key: metadata},
            init=False,
            repr=False,
            compare=False,
        )

    @classmethod
    def parse(cls: typing.Type[M], data: dict[str, Any], **kwargs: str) -> M:
        """
        Parses data into the model based on the field definitions
        """
        params = {}

        for f, metadata in cls._data_fields():
            value = cls._get_value(data=data, f=f, metadata=metadata)

            params[f.name] = value

        return cls(**params, **kwargs)

    def marshal(self) -> dict[str, Any]:
        """
        Marshals the model into a dictionary based on the fields (excluding composite) and views
        """

        def nested_default() -> defaultdict[str, Any]:
            """
            Returns a recursive defaultdict useful for creating nested dicts
            based on a specified path.
            """
            return defaultdict(nested_default)

        result: dict[str, Any] = {}

        for f, metadata in self._view_fields():
            if metadata.view_parts:
                # compose the parts, joining with the composite_key_delimiter
                result_value = self.composite_key_delimiter.join(
                    str(getattr(self, attribute)) for attribute in metadata.view_parts
                )
            else:
                # just use the attribute value
                result_value = getattr(self, f.name)

            if result_value != f.default or f.default is not None:
                # value actually exists, so it needs to be placed in the proper path
                temp_result = nested_default()

                # get the dictionary item where the value should be placed
                result_item: dict[str, Any] = reduce(
                    operator.getitem, metadata.path[:-1], temp_result
                )

                # cast to the correct type if it's not already
                result_value = self._cast_value(f.type, result_value)
                if metadata.marshal_as is not None:
                    result_value = metadata.marshal_as(result_value)

                # set the value
                result_item[metadata.path[-1]] = result_value

                # convert all defaultdicts to dict
                temp_result = json.loads(json.dumps(temp_result))

                # include the path and value in the result
                result |= temp_result

        return result

    @classmethod
    def _data_fields(cls) -> typing.Generator[tuple[Any, _Metadata], None, None]:
        for f in fields(cls):
            metadata = cls._get_metadata(f)
            if metadata is not None and not metadata.view_parts:
                yield f, metadata

    @classmethod
    def _view_fields(cls) -> typing.Generator[tuple[Any, _Metadata], None, None]:
        for f in fields(cls):
            metadata = cls._get_metadata(f)
            if metadata is not None and metadata.composite_index is None:
                yield f, metadata

    @classmethod
    def _get_value(cls, data: dict[str, Any], f: Any, metadata: _Metadata) -> Any:
        value: f.type = None
        if f.default is None:
            try:
                value = reduce(operator.getitem, metadata.path, data)
            except KeyError:
                pass
        else:
            value = reduce(operator.getitem, metadata.path, data)

        if metadata.composite_index is not None:
            value = str(value).split(cls.composite_key_delimiter)[
                metadata.composite_index
            ]

        value = cls._cast_value(f.type, value)

        return value

    @classmethod
    def _cast_value(cls, t: type, value: Any) -> Any:
        types = typing.get_args(t) if typing.get_args(t) else (t,)
        if not isinstance(value, types):
            for t in types:
                if t is not None:
                    return t(value)
        return value

    @classmethod
    def _get_metadata(cls, f: Any) -> _Metadata | None:
        if (
            f.metadata
            and cls._metadata_key in f.metadata
            and isinstance(f.metadata[cls._metadata_key], _Metadata)
        ):
            return typing.cast(_Metadata, f.metadata[cls._metadata_key])
        return None
