#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# copy from https://gist.github.com/JJGO/e64c0e8aedb5d464b5f79d3b12197338

import sys
import shutil
from typing import Optional, List, Tuple, Dict

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install

import re

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "ROLE": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEADER": "#d0b343",
    "TERM": "#70c43f",
    "RPC": "#4878bc",
    "TIMER": "#398280",
    "COMMIT": "#98719f",
    "APPLY": "#d08341",
    "SNAPSHOT": "#FD971F",
    "HEART": "#ff615c",
    "TICKER": "#00813c",
    "TEST": "#fe2c79",
    "CLIENT": "#ffffff",
    "WARN": "#d08341",
    "ERROR": "#fe2626",
    "TRACE": "#fe2626",
    "start": "#fe2626",
    "server": "#fe2626",
    "STATE": "#d00275",
    "ELECT": "#35adae",
}
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def main(
    file:      typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize:  bool           = typer.Option(True, "--no-color"),
    n_columns: Optional[int]  = typer.Option(None, "--columns", "-c"),
    ignore:    Optional[str]  = typer.Option(None, "--ignore", "-i", callback=list_topics),
    just:      Optional[str]  = typer.Option(None, "--just", "-j", callback=list_topics),
):
    topics = list(TOPICS)

    log_re = re.compile('^\[.*\]\[(?P<time>.*)s\]\[(?P<ip>.*)\]\[.*\] (?P<topic>\w+) (?P<msg>.*)')


    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    for line in input_:
        try:
            res = log_re.match(line)
            # if res is None:
            #     print('Yeah', res.group(), res.groupdict())
                # continue
            d = res.groupdict()

            time  = d['time']
            ip    = d['ip']
            topic = d['topic']
            msg   = d['msg'].strip()

            # time, topic, *msg = line.strip().split(" ")
            # To ignore some topics
            if topic not in topics:
                print(f'skipped topic: {topic}')
                continue

            # msg = " ".join(msg)

            # Debug calls from the test suite aren't associated with
            # any particular peer. Otherwise we can treat second column
            # as peer id
            if topic not in ["TEST", "start", "server"]:
                i = int(msg[1])

            # Colorize output by using rich syntax when needed
            if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            # Single column printing. Always the case for debug stmts in tests
            if n_columns is None or topic.upper() in ["TEST", "START", "SERVER"]:
                print(time, topic, msg)
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
            else:
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                print(cols)
        except:
            # Code from tests or panics does not follow format
            # so we print it as is
            if line.startswith("panic"):
                panic = True
            # Output from tests is usually important so add a
            # horizontal line with hashes to make it more obvious
            if not panic:
                print("#" * console.width)
            print(line, end="")


if __name__ == "__main__":
    typer.run(main)
