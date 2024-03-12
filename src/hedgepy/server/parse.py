from dataclasses import fields
from hedgepy.common import template
from hedgepy.server.bases import Resource


def _post_process_template(template: dict) -> dict:
    name, resources = template.popitem()
    common, resources = resources[0], resources[1:]
    for resource in resources:
        for attr in fields(Resource):
            if (not getattr(resource, attr.name)) and (common_value := getattr(common, attr.name)):
                setattr(resource, attr.name, common_value)
    post_processed = {name: resources}
    return post_processed


def _process_node(node: dict) -> Resource:
    return Resource(**node)


def _process_template(template: dict) -> tuple[Resource]:
    outer_node_di, inner_node_li = template["common"], template["templates"]
    processed = (_process_node(outer_node_di),)
    for inner_node_di in inner_node_li:
        processed += (_process_node(inner_node_di),)
    return processed    


def process_templates(templates: dict) -> dict[str, tuple[Resource]]:
    processed = {}
    while templates:
        template_name, template = templates.popitem()
        processed[template_name] = _process_template(template)
    post_processed = _post_process_template(processed)
    return post_processed


def main():
    templates = template.get_templates()
    processed = process_templates(templates)
    return processed


if __name__ == "__main__":
    main()
        