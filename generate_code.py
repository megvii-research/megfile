import inspect
import json
import typing

ALL_TYPES = ['fs', 's3', 'http', 'stdio']


with open('megfile/__init__.py', 'w') as init_file:
    init_file.truncate(0)


def get_all_functions_from_types():
    from megfile import fs, s3, http, stdio

    all_type_functions = {}
    all_functions_group_by_type = {t: [] for t in ALL_TYPES}
    for current_type in ALL_TYPES:
        for obj_name, obj in inspect.getmembers(locals()[current_type]):
            if inspect.isfunction(obj) and obj_name.startswith(f"{current_type}_"):
                all_current_func_parameter = {}
                for param_name, param_obj in inspect.signature(obj).parameters.items():
                    if param_name in ['kwargs', 'args']:
                        continue
                    all_current_func_parameter[param_name] = {"default": param_obj.default, "annotation": param_obj.annotation, "kind": param_obj.kind}
                if 'path' not in all_current_func_parameter and 'src_path' not in all_current_func_parameter:
                    continue
                base_func_name = obj_name[len(f"{current_type}_"):]
                if base_func_name not in all_type_functions:
                    all_type_functions[base_func_name] = {}
                all_type_functions[base_func_name][current_type] = {"parameters": all_current_func_parameter, "return_annotation": inspect.signature(obj).return_annotation}
                all_functions_group_by_type[current_type].append(obj_name)
    return all_type_functions, all_functions_group_by_type


def string_typing(annotation):
    if hasattr(annotation, '__name__') and hasattr(annotation, '__module__') and annotation.__module__ != 'builtins':
        s = f'{annotation.__module__}.{annotation.__name__}'
    elif hasattr(annotation, '__name__'):
        s = annotation.__name__
    elif isinstance(annotation, str):
        s = f"'{annotation}'"
    else:
        s = str(annotation)
    return s.replace('NoneType', 'None').replace('_io', 'io').replace('~', 'typing.')


def sort_params(t):
    if 'default' in t[1]:
        return 2
    elif t[1].get('kind') == inspect.Parameter.VAR_KEYWORD:
        return 3
    elif t[1].get('kind') == inspect.Parameter.VAR_POSITIONAL:
        return 1
    else:
        return 0


def get_template_smart_file_content():
    exist_func_set = set()
    template_function_content_lines = []
    import_lines = []
    with open('template/smart.py', 'r') as base_smart_file:
        ignoring = False
        for line in base_smart_file.readlines():
            if line.strip() == '# auto-smart-ignore-start':
                ignoring = True
                continue
            elif line.strip() == '# auto-smart-ignore-end':
                ignoring = False
                continue
            elif ignoring is True:
                continue
            if line.startswith('def '):
                func_name = line.split('(')[0].replace('def ', '').strip()
                exist_func_set.add(func_name)
            if line.startswith("import") or line.startswith("from"):
                import_lines.append(line)
            else:
                template_function_content_lines.append(line)
    return exist_func_set, import_lines, template_function_content_lines


def join_params_and_return_annotation(func_info):
    current_parameters, return_annotation = {}, inspect._empty
    for parameter_info in func_info.values():
        if parameter_info['return_annotation'] is not inspect._empty:
            if return_annotation is inspect._empty:
                return_annotation = parameter_info['return_annotation']
            else:
                return_annotation = typing.Union[return_annotation, parameter_info['return_annotation']]
        for parameter, param_info in parameter_info['parameters'].items():
            if parameter not in current_parameters:
                current_parameters[parameter] = {'kind': param_info['kind']}
            if param_info['default'] is not inspect._empty and 'default' not in current_parameters[parameter]:
                current_parameters[parameter]['default'] = param_info['default']
            if param_info['annotation'] is not inspect._empty:
                if 'annotation' not in current_parameters[parameter]:
                    current_parameters[parameter]['annotation'] = param_info['annotation']
                else:
                    current_parameters[parameter]['annotation'] = typing.Union[current_parameters[parameter]['annotation'], param_info['annotation']]
    return current_parameters, return_annotation


def main():
    all_type_functions, all_functions_group_by_type = get_all_functions_from_types()
    joined_type_functions, joined_functions_group_by_type = {}, {}
    for base_func_name, func_info in list(all_type_functions.items()):
        if len(list(func_info.keys())) > 1:
            joined_type_functions[base_func_name] = func_info
            for current_type in func_info.keys():
                if current_type not in joined_functions_group_by_type:
                    joined_functions_group_by_type[current_type] = []
                joined_functions_group_by_type[current_type].append(f"{current_type}_{base_func_name}")

    smart_functions = []
    with open('megfile/smart.py', 'w') as smart_file:

        # write template smart file imports
        exist_func_set, import_lines, template_function_content_lines = get_template_smart_file_content()
        smart_functions.extend([func_name for func_name in exist_func_set if func_name.startswith('smart_')])
        for current_type, function_names in joined_functions_group_by_type.items():
            smart_file.write('from megfile.%s import %s\n' % (current_type, ", ".join([name for name in function_names if f"smart_{name[len(current_type)+1:]}" not in exist_func_set])))
        for import_line in import_lines:
            smart_file.write(import_line)
        smart_file.write('\n\n')

        # join and write functions
        for func_name, func_info in joined_type_functions.items():

            if f"smart_{func_name}" in exist_func_set:
                continue

            current_parameters, return_annotation = join_params_and_return_annotation(func_info)
            current_parameters_list = sorted([(name, info) for name, info in current_parameters.items()], key=sort_params)
            if 'path' in current_parameters:
                path_param_name = "path"
            elif 'src_path' in current_parameters:
                path_param_name = "src_path"
            else:
                continue
            
            # write function name
            smart_file.write(f'def smart_{func_name}(')
            smart_functions.append(f'smart_{func_name}')

            # write function parameters
            all_param = []
            for param_name, param_info in current_parameters_list:
                if param_info['kind'] == inspect.Parameter.VAR_POSITIONAL:
                    param_str = [f"*{param_name}"]
                elif param_info['kind'] == inspect.Parameter.VAR_KEYWORD:
                    param_str = [f"**{param_name}"]
                else:
                    param_str = [param_name]
                if 'annotation' in param_info:
                    param_str.append(f": {string_typing(param_info['annotation'])}")
                if 'default' in param_info:
                    param_str.append(f" = {string_typing(param_info['default'])}")
                all_param.append("".join(param_str))
            smart_file.write(", ".join(all_param))
            smart_file.write(')')

            # write function return_annotation
            if return_annotation is not inspect._empty:
                smart_file.write(f" -> {string_typing(return_annotation)}:")
            else:
                smart_file.write(":")
            smart_file.write("\n")

            smart_file.write(" " * 4)
            smart_file.write(f"protocol = _extract_protocol({path_param_name})\n")

            # write function content
            for current_type, parameter_info in func_info.items():
                smart_file.write(" " * 4)
                smart_file.write(f"if protocol == '{current_type}':\n")
                smart_file.write(" " * 8)
                smart_file.write(f"return {current_type}_{func_name}(")
                parameter_list = []
                for parameter, param_info in parameter_info['parameters'].items():
                    if param_info['kind'] == inspect.Parameter.VAR_POSITIONAL:
                        parameter_list = [i.split('=')[0] for i in parameter_list]
                        parameter_list.append(f"*{parameter}")
                    elif param_info['kind'] == inspect.Parameter.VAR_KEYWORD:
                        parameter_list.append(f"**{parameter}")
                    else:
                        parameter_list.append(f"{parameter}={parameter}")
                smart_file.write(", ".join(parameter_list))
                smart_file.write(')\n')
            smart_file.write(" " * 4)
            smart_file.write(f"raise UnsupportedError(operation='smart_{func_name}', path={path_param_name})\n\n\n")

        # write template smart file functions
        smart_file.writelines(template_function_content_lines)

    with open('megfile/__init__.py', 'w') as init_file:
        init_file.write("from megfile.version import VERSION as __version__\n")

        all_func_list = []
        for current_type, function_names in all_functions_group_by_type.items():
            all_func_list.extend(function_names)
            init_file.write('from megfile.%s import %s\n' % (current_type, ", ".join(function_names)))
        init_file.write('from megfile.smart import %s\n\n\n' % ", ".join(smart_functions))
        all_func_list.extend(smart_functions)
        init_file.write("__all__ = ")
        init_file.write(json.dumps(all_func_list, indent=2).replace('\n]', ',\n]'))
        pass

if __name__ == "__main__":
    main()