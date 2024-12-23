
from __future__ import annotations

import ezpyzy as ez
import dataclasses as dc


with ez.test('Define Table layout'):
    @dc.dataclass
    class Turn(ez.Row):
        dialogue: ez.Col[int] = None
        index: ez.Col[int] = None
        speaker: ez.Col[str] = None
        text: ez.Col[str] = None
        domains: ez.Col[list[str]] = ez.default([])

with ez.test('Construct a normal Turn object'):
    turn = Turn(0, 0, 'user', 'Hello!')
    assert dict(vars(turn)) == dict(dialogue=0, index=0, speaker='user', text='Hello!', domains=[]) # noqa

with ez.test('Construct a table of turn objects'):
    turns_from_turns = Turn.s(
        Turn(0, 0, 'user', 'Hello!'),
        Turn(0, 1, 'bot', 'Hi'),
        Turn(0, 1, 'user', 'Can you help me?')
    )

with ez.test('Construct a Turn Table from lists'):
    turns_from_lists = Turn.s(
        [0, 0, 'user', 'Hello!'],
        [0, 1, 'bot', 'Hi'],
        [0, 1, 'user', 'Can you help me?']
    )
    
with ez.test('Construct a Turn Table from dicts'):
    turns_from_dicts = Turn.s(
        dict(speaker='user', text='Hello!', dialogue=0, index=0),
        dict(speaker='bot', text='Hi', dialogue=0, index=0),
        dict(speaker='user', text='Can you help me?', dialogue=0, index=0)
    )
    
with ez.test('Construct a Turn Table from column lists'):
    turns_from_col_lists = Turn.s(
        dialogue=[0, 0, 0],
        index=[0, 0, 0],
        speaker=['user', 'bot', 'user'],
        text=['Hello!', 'Hi', 'Can you help me?']
    )

