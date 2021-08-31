#!/bin/bash
cd ..
cp {{ cookiecutter.slug }}/{{ cookiecutter.slug }}.py src/shillelagh/adapters/{{ cookiecutter.adapter_type }}/
cp {{ cookiecutter.slug }}/test_{{ cookiecutter.slug }}.py tests/adapters/{{ cookiecutter.adapter_type }}/
rm -rf {{ cookiecutter.slug }}
