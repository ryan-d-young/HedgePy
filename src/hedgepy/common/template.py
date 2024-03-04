import os
import json
import jsonschema
from pathlib import Path


ROOT = Path(os.getcwd()) / "src" / "hedgepy" / "templates"
VALIDATOR = jsonschema.Draft202012Validator


def validate(schema: dict | None = None, instance: dict | None = None) -> Exception | None:
    schema = get_schema() if not schema else schema
    
    if instance:
        try:
            jsonschema.validate(
                instance=instance,
                schema=schema,
                cls=VALIDATOR,
                format_checker=VALIDATOR.FORMAT_CHECKER)
        except jsonschema.exceptions.ValidationError as e:
            return e
    else:
        try:
            VALIDATOR.check_schema(schema)
        except jsonschema.exceptions.SchemaError as e:
            return e


def get_schema() -> dict:
    with open(ROOT / "_schema.json") as file:
        schema = json.load(file)
    if not (e := validate(schema)):
        return schema
    else:
        raise e


def get_template(template_name: str) -> dict:
    schema = get_schema()
    with open(ROOT / f"{template_name}.json") as file:
        template = json.load(file)
    if not (e := validate(schema, template)):
        return template
    else:
        raise e


def get_templates() -> dict:
    return dict(
        zip(map(lambda x: x.stem, ROOT.glob("*.json")),
            map(lambda x: get_template(x.stem), ROOT.glob("*.json")))
    )


def put_template(template_name: str, template: dict):
    schema = get_schema()
    if not (e := validate(schema, template)):
        if (fp := ROOT / f"{template_name}.json").exists():
            os.remove(fp)
        with open(fp, "x") as f:
            f.write(json.dumps(template, indent=4))
    else:
        raise e


def create_template(template_name: str, template: dict):
    if not (ROOT / f"{template_name}.json").exists():
        put_template(template_name, template)
    else:
        raise NameError("Template already exists")
