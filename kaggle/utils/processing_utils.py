import json
import itertools
import re
from tqdm import tqdm
import sys
from typing import List
from bs4 import BeautifulSoup
import random
import keyword
import signal

DEFAULT_OUTPUT = "<empty>"
DEFAULT_TABLE_TEMPLATE = "{}"
# truncate the part after 200 tokens
MAX_OUTPUT_LEN = 3 * 200
# if longer than 200 tokens, we will drop the line since it is abnormal
MAX_CODE_LEN = 3 * 200

MAX_TABLE_COLUMN, MAX_TABLE_ROW = 5, 5

def parse_html_table(html):
    def normalize_cell_text(text):
        return text.strip().replace('\n', ' ').replace('|', '&#124;')

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')

    if not table:
        return ''

    rows = table.find_all('tr')
    headers = [normalize_cell_text(cell.get_text()) for cell in rows[0].find_all('th')]
    rows = [[normalize_cell_text(cell.get_text()) for cell in row.find_all('td')] for row in rows[1:]]

    if len(rows) > MAX_TABLE_ROW:
        # random select 10 rows
        rows = random.sample(rows, 5)

    # if the columns are too many, then we only select the first 10 columns
    if len(headers) > MAX_TABLE_COLUMN:
        headers = headers[:5]
        rows = [[row[i] for i in range(5)] for row in rows]

    if len(headers) > 0:
        markdown_table = ['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |']
    else:
        markdown_table = []
    for row in rows:
        if len(row):
            markdown_table.append('| ' + ' | '.join(row) + ' |')

    if len(markdown_table):
        return DEFAULT_TABLE_TEMPLATE.format('\n'.join(markdown_table))
    else:
        return ''


def is_single_variable(line):
    line = line.strip()

    # check if the line is a single variable
    pattern = r'^\w+[\.\w+\(\)\d]*$'
    match = re.match(pattern, line)

    # if match and line not in keyword.kwlist
    if match and line not in keyword.kwlist:
        return True
    else:
        return False


def filter_unused_output(plain_text: List):
    content = "".join(plain_text)
    # filter out progress bar
    if ", ?B/s]" in content:
        return False
    if "................" in content:
        return False
    return True


def clean_code(input_code: str):
    # TODO: we can extract the in-line comment and place them in the above line
    input_code = re.sub("\n(\n+)", "\n\n", input_code)
    code_lines = input_code.split("\n")
    black_phrase_list = [
        "This Python 3 environment comes",
        "here's several helpful packages to load",
        "defined by the kaggle/python docker image",
        "defined by the kaggle/python Docker image",
        "Copyright (C)",
    ]
    for idx in range(len(code_lines)):
        line = code_lines[idx].strip()
        # magic command
        if line.startswith("%") or line.startswith("!") or line.startswith("cd ") \
                    or line.startswith("pip ") or line.startswith("apt ") or line.startswith("wget "):
            code_lines[idx] = ""
        elif len(line) > MAX_CODE_LEN:
            # unexpected long, just drop it
            code_lines[idx] = ""
        for phrase in black_phrase_list:
            if phrase in line:
                code_lines[idx] = ""
        # TODO: single variable, and we should be careful about the plt.show() case
        # elif is_single_variable(line):
        #     code_lines[idx] = "print({})".format(line)
    code_lines = [line for line in code_lines if line.strip() != ""]
    # since we will have a unified formatter latter, so we don't need to worry about the space
    # TODO: autopep8 will be stuck sometimes, so we need to set a timeout in the future
    # return autopep8.fix_code("\n".join(code_lines))
    return "\n".join(code_lines)


def timeout_handler(signum, frame):
    # Handle the action to be performed after the timeout triggers
    # You can raise an exception or perform any other desired action
    raise TimeoutError("Timeout occurred")


def set_timeout(seconds):
    # Register the timeout handler function
    signal.signal(signal.SIGALRM, timeout_handler)
    # Set the timeout duration
    signal.alarm(seconds)


def clean_output(outputs: List):
    output_text = ""
    if len(outputs) > 0:
        # deal with figure
        for output in outputs:
            if 'data' in output.keys():
                all_data_keys = output['data'].keys()
                # fetch text
                for key in all_data_keys:
                    # there will be always a html field, it yes then parse it
                    # if not, then just use the plain text
                    content = "".join(output['data'][key])
                    if key == "text/html" and ("dataframe" in content or "<table" in content):
                        # give at most 10 seconds to parse the table
                        set_timeout(10)
                        try:
                            # sometimes bs4 will be stuck, so we need to set a timeout
                            result = parse_html_table(content)
                        except TimeoutError as e:
                            print("Timeout occurred in parsing html table")
                        else:
                            output_text += result
                        finally:
                            signal.alarm(0)
                            break
                    elif key in ["text/markdown", "text/latex"]:
                        output_text += "".join(content)
                        # randomly drop the output
                        output_text = DEFAULT_OUTPUT if random.random() < 0.2 else output_text
                        break
                    elif key in ["application/javascript",
                                 # TODO: this field indicates the possible interactive tutorial
                                 # ['parent.postMessage({"jupyterEvent": "custom.exercise_interaction", "data": {"outcomeType": 1, "valueTowardsComplet
                                 # ion": 0.3333333333333333, "interactionType": 1, "questionType": 2, "learnTutorialId": 110, "questionId": "1_EarlyExi
                                 # tDebugging", "learnToolsVersion": "0.3.2", "failureMessage": "", "exceptionClass": "", "trace": ""}}, "*")']
                                 "application/vnd.jupyter.widget-view+json",
                                 # {'model_id': 'd78d9bdce1344ecab6ccc6e64b0d03f2', 'version_major': 2, 'version_minor': 0}
                                 "image/png",
                                 "image/jpeg",
                                 "application/vnd.plotly.v1+json",
                                 "text/vnd.plotly.v1+html",
                                 "image/svg+xml",
                                 "application/vnd.bokehjs_exec.v0+json",
                                 "application/vnd.bokehjs_load.v0+json"]:
                        # we rely on the text part to give some hints
                        pass
                    elif key not in ["text/plain", "text/html"]:
                        print("Unknown key: {}".format(key))

                    if key == "text/plain" or key == "text/html":
                        # remove meaningless output
                        if filter_unused_output(output['data'][key]):
                            # every line already has a \n at the end
                            output_text += "".join(output['data'][key]).strip("\n") + "\n"
                        # escape """
                        output_text = output_text.replace('"""', '')
                        # TODO Qian: randomly set 20% output as the default output
                        output_text = DEFAULT_OUTPUT if random.random() < 0.2 else output_text

            elif 'text' in output.keys():
                output_text += "".join(output['text']).strip("\n") + "\n"
    output_text = output_text.strip("\n")
    # if output_text is too long, truncate it
    if len(output_text) > MAX_OUTPUT_LEN:
        output_text = output_text[:MAX_OUTPUT_LEN] + "..."
    # set a default value
    if output_text == "":
        output_text = DEFAULT_OUTPUT
    return output_text


def clean_markdown(text):
    text = re.sub(r'<.*?>', '', text)
    text = re.sub(r'\r\n', '\n', text)
    text = re.sub(r'\n+', '\n', text)
    # TODO: we want to keep the hirechical structure of markdown
    # text = text.replace('#', '')
    # IntroductionGreetings from the Kaggle bot!  ...
    black_phrase_list = [
        "IntroductionGreetings from the Kaggle bot!",
        "Kaggle bot",
        "Kaggle kerneler bot",
        "automatically-generated",
        "clicking run or pressing Shift+Enter",
        "https://www.kaggle.com/learn/python",
        "Thank You",
    ]
    for phrase in black_phrase_list:
        if phrase in text:
            text = ""
    return text


def segment_blocks(content):
    cells = []
    cell_types = []
    for cell in content:
        if len(cell['source']) > 0:
            output = DEFAULT_OUTPUT
            if 'outputs' in cell.keys():
                output = clean_output(cell['outputs'])
            cells.append({"input": ''.join(cell['source']),
                          "output": output})
            cell_types.append(cell['cell_type'])
    # if the current cell is empty, then merge it with the next cell if they have the same type
    for i in range(len(cells) - 1):
        if cells[i]["output"] == DEFAULT_OUTPUT and cell_types[i] == cell_types[i + 1]:
            separator = '\n'
            cells[i + 1]["input"] = cells[i]["input"] + separator + cells[i + 1]["input"]
            cells[i]["input"] = ''
            cell_types[i] = ''
    cells = [cell for cell in cells if cell["input"] != '']
    cell_types = [cell_type for cell_type in cell_types if cell_type != '']
    return cells, cell_types


def formatter(content, option):
    assert option in ['code', 'markdown', 'result', 'raw'], "Unknown option: {}".format(option)
    if option == 'code':
        return clean_code(content)
    elif option == 'markdown':
        content = clean_markdown(content)
        if content != "":
            return "\n".join(["# " + line.strip() for line in content.split("\n")])
        else:
            return ""
    elif option == 'result' and content != DEFAULT_OUTPUT:
        result_lines = content.split("\n")
        if len(result_lines) >= 5:
            result_lines = result_lines[:5] + ["..."]
        wrapper = '"""Example Output:\n{}\n"""'
        content = wrapper.format("\n".join(result_lines))
        return content
    else:
        return ""


def count_ratio_of_markdown_cells(types):
    # statics the ratio of markdown cells
    markdown_count = 0
    for cell_type in types:
        if cell_type == "markdown":
            markdown_count += 1
    return markdown_count / len(types)




def parse_jupyter_into_script(notebook_json_str, use_code_execution):
    """
    Why we do not use jupytext is that we want to keep the output results of the notebook
    """
    try:
        notebook = json.loads(notebook_json_str)
        script_content = ""
        conversation_text = ""
        # add the filtering: notebook without more than 4 cells will be ignored
        if len(notebook) < 4:
            return ""

        cells, types = segment_blocks(notebook)
        if "code" not in types:
            # no code, no need to parse
            return ""
        # follow paper https://arxiv.org/abs/2201.12901
        # here we remove the jupyter notebook whose markdown cells are less than 30%
        # TODO: after discussion with pengcheng, we do not use this by now
        # if count_ratio_of_markdown_cells(types) < 0.3:
        #     return ""
        # flatten the list of cells to incorporate markdown and code

        for i in range(len(cells)):
            # if this is the last cell and it is a markdown cell, then we do not need to parse it
            if i == len(cells) - 1 and types[i] == 'markdown':
                break
            cell, cell_type = cells[i], types[i]
            if cell['output'] == "<empty>":
                cell['output'] = ""
            cell_script = ""
            text_code_part = formatter(cell['input'], cell_type)
            
            # if do not use, then set it as empty
            if use_code_execution:
                result_part = formatter(cell['output'], 'result')
            else:
                result_part = ""

            if result_part != "" and text_code_part != "":
                cell_script = text_code_part + "\n" + result_part + "\n\n"
            elif len(cell['output']) != 0 and cell_type == 'markdown':
                # the markdown indicates an interactive widget but we cannot show it now, so ignore it
                pass
            elif result_part == "" and text_code_part != "":
                cell_script = text_code_part + "\n"
                # markdown should be separated with the previous code
                if cell_type == 'markdown':
                    cell_script = "\n" + cell_script
                if "def " in text_code_part:
                    # excpliitly add a new line between the current code to the next markdown
                    # if the current code has a function definition, then we also add a new line
                    cell_script += "\n"

            conversation_text += cell_script
        script_content = conversation_text
        return script_content
    except Exception as e:
        print("Failed to parse the notebook: {}".format(e))
        # traceback.print_exc()
        return ""
