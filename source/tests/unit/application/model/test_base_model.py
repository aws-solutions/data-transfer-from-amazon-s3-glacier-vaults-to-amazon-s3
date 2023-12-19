"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import dataclasses
from datetime import datetime
from typing import Any, Type

import pytest

from solution.application.model import base


@pytest.fixture
def data() -> dict[str, Any]:
    return dict(
        A=dict(B="nested"),
        C="single",
    )


@pytest.fixture
def model_class(
    path: str | list[str],
    optional: bool,
) -> type:
    if optional:

        @dataclasses.dataclass
        class COptional(base.Model):
            c: str | None = base.Model.field(path=path, optional=optional)
            extra: Any | None = dataclasses.field(default=None)

        return COptional

    @dataclasses.dataclass
    class C(base.Model):
        c: str = base.Model.field(path=path)
        extra: Any | None = dataclasses.field(default=None)

    return C


@pytest.mark.parametrize(
    "path,expected", [("C", "single"), (["A", "B"], "nested")], ids=["single", "nested"]
)
@pytest.mark.parametrize("optional", [True, False], ids=["optional", "required"])
@pytest.mark.parametrize(
    "extra,expected_extra",
    [({}, None), ({"extra": "foo"}, "foo")],
    ids=["no extra", "extra"],
)
def test_parsing(
    data: dict[str, Any],
    path: str | list[str],
    expected: str,
    optional: bool,
    extra: dict[str, str],
    expected_extra: Any,
    model_class: Type[base.Model],
) -> None:
    parsed = model_class.parse(data, **extra)
    assert parsed.c == expected  # type: ignore
    assert parsed.extra == expected_extra  # type: ignore

    if not optional:
        with pytest.raises(KeyError):
            model_class.parse({})
    else:
        parsed = model_class.parse({})
        assert parsed.c is None  # type: ignore


@pytest.mark.parametrize(
    "path,expected", [("C", "single"), (["A", "B"], "nested")], ids=["single", "nested"]
)
@pytest.mark.parametrize("optional", [True, False], ids=["optional", "required"])
@pytest.mark.parametrize(
    "extra,expected_extra",
    [({}, None), ({"extra": "foo"}, "foo")],
    ids=["no extra", "extra"],
)
def test_marshaling(
    data: dict[str, Any],
    path: str | list[str],
    expected: str,
    optional: bool,
    extra: dict[str, str],
    expected_extra: Any,
    model_class: Type[base.Model],
) -> None:
    parsed = model_class.parse(data, **extra)

    assert model_class.parse(parsed.marshal()).marshal() == parsed.marshal()

    if optional:
        assert model_class.parse({}).marshal() == {}


def test_composite_key() -> None:
    @dataclasses.dataclass
    class C(base.Model):
        composite_part_1: str = base.Model.field(["A", "B"], composite_index=0)
        composite_part_2: str = base.Model.field(["A", "B"], composite_index=1)

        @property
        def key(self) -> str:
            return self.composite_key_delimiter.join(
                (self.composite_part_1, self.composite_part_2)
            )

    data = dict(A=dict(B="foo|bar"))

    c = C.parse(data)

    assert c.composite_part_1 == "foo"
    assert c.composite_part_2 == "bar"
    assert c.key == "foo|bar"


def test_reusing_composite_parts() -> None:
    data = dict(A=dict(B="foo|bar|2"), C="bar|foo", D=2)

    @dataclasses.dataclass
    class C(base.Model):
        attribute_1: str = base.Model.field(["A", "B"], composite_index=0)
        attribute_2: str = base.Model.field(["A", "B"], composite_index=1)
        attribute_3: int | None = base.Model.field(
            ["A", "B"], composite_index=2, optional=True
        )
        _b: str = base.Model.view(
            ["A", "B"], ["attribute_1", "attribute_2", "attribute_3"]
        )
        _c: str = base.Model.view(["C"], ["attribute_2", "attribute_1"])
        _d: int = base.Model.view("D", "attribute_3")

    c = C.parse(data)

    assert c.attribute_1 == "foo"
    assert c.attribute_2 == "bar"
    assert c.attribute_3 == 2
    with pytest.raises(AttributeError):
        assert c._b is None
    with pytest.raises(AttributeError):
        assert c._c is None
    with pytest.raises(TypeError):
        C(attribute_1="foo", attribute_2="bar", attribute_3=2, _b="no assign")

    assert c.marshal() == data

    new_c = C(attribute_1="foo", attribute_2="bar", attribute_3=2)
    assert new_c.marshal() == data


def test_defaults() -> None:
    @dataclasses.dataclass
    class Entity(base.Model):
        identifier: str = base.Model.field(["pk", "S"], composite_index=0)
        timestamp: str = base.Model.field(
            ["sk", "S"], default_factory=lambda: datetime.now().isoformat()
        )
        type: str = base.Model.field(["type"], default="type")
        _pk: str = base.Model.view(["pk", "S"], ["identifier", "date"])

        @property
        def key(self) -> dict[str, Any]:
            return {k: v for k, v in self.marshal().items() if k in ("pk", "sk")}

        @property
        def date(self) -> str:
            return self.timestamp[:10]

    entity = Entity(identifier="123")
    entity2 = Entity(identifier="123")

    assert entity != entity2
    assert entity.marshal() != entity2.marshal()
    assert entity.type == "type"
    assert entity.marshal()["type"] == "type"

    entity.timestamp = "2023-07-13T11:23:20.387474"
    assert entity.key == {
        "pk": {"S": "123|2023-07-13"},
        "sk": {"S": "2023-07-13T11:23:20.387474"},
    }


def test_casting_int_to_string_when_marshaling() -> None:
    @dataclasses.dataclass
    class Entity(base.Model):
        value: int = base.Model.field(["value", "N"], marshal_as=str)
        _value: int = base.Model.view(
            ["value_view", "N"], parts=["value"], marshal_as=str
        )

    entity = Entity(value=2)
    assert entity.marshal()["value"]["N"] == "2"
    assert entity.marshal()["value_view"]["N"] == "2"
    assert Entity.parse({"value": {"N": "2"}}).value == 2


def test_default_inheritance() -> None:
    @dataclasses.dataclass
    class Entity(base.Model):
        default: str = base.Model.field(["default"], default="default")

    @dataclasses.dataclass
    class Derived(Entity):
        required: str = base.Model.field(["required"])

    derived = Derived(required="foo")
    assert derived.default == "default"
