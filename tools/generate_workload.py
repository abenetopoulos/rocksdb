#!/usr/bin/env python3
"""
Script to generate loads to be used for benchmarking.

Usage:
    generate_workload.py [-h] [-o file] [-k keys] [-n ops] [--percentage-reads=<pr>] [--max-read-distance=<rd>]

Options:
  -o, --output file  File to write output to (default `/tmp/rocksdb/workload`)
  -k, --num-keys keys  Number of distinct keys to include in the workload (default 1000)
  -n, --num-ops ops  Number of operations to include in the workload's main section (default 10 * NUM_KEYS)
  --percentage-reads=<pr>  Percentage of main workload instructions to dedicate to pure reads [default: 0.5]
  --max-read-distance=<rd>  Maximum number of operations that can exist between a write for a key and a read for the same key [default: 64]
  -h, --help  Print this help dialog.
"""
import decimal
import enum
from functools import reduce
import os
import random
from typing import Dict, List, Optional, Set


from docopt import docopt

# TODOs
#   - replace `print()` calls with an actual logger
#   - figure out a better way to introduce skew into our generation algorithm

######################################################
# Data definitions
######################################################

_OUTPUT_FILE_FLAG = '--output'
_NUM_KEYS_FLAG = '--num-keys'
_NUM_OPS_FLAG = '--num-ops'
_PERCENTAGE_READS_FLAG = '--percentage-reads'
_MAX_READ_DISTANCE_FLAG = '--max-read-distance'

_DEFAULT_OUTPUT_FILE = '/tmp/rocksdb/workload'


class CommandKind(enum.Enum):
    WRITE = 'w'
    READ = 'r'
    DELETE = 'd'

    def to_str(self) -> str:
        return self.value


class Command:
    kind: CommandKind
    key: str

    value: Optional[str]

    def __init__(self, kind, key, value=None):
        self.kind = kind
        self.key = key
        self.value = value

    def to_str(self) -> str:
        return '{} {} {}'.format(self.kind.to_str(), self.key, self.value if self.value else '')


class Parameters:
    output_path: str
    num_keys: int
    num_ops: int
    percentage_reads: decimal.Decimal

    # number that tells us for how many operations after a value was written we should
    # try to generate a `read` instruction for it. This is done in an attempt to introduce
    # skew:
    #   - a low value would mean that keys we write are read very rarely and for a very short
    #     time after writing them
    #   - a high value increases the potential lifetime of a written key.
    max_read_distance: int

    def __init__(self, output_path=_DEFAULT_OUTPUT_FILE):
        self.output_path = output_path
        self._output_file = None

    def populate_from_arguments(self, arguments: dict):
        arg_file_path = arguments[_OUTPUT_FILE_FLAG]
        if arg_file_path:
            self.output_path = arg_file_path

        num_keys = arguments[_NUM_KEYS_FLAG]
        if num_keys:
            self.num_keys = int(num_keys)
        else:
            self.num_keys = 1000

        num_ops = arguments[_NUM_OPS_FLAG]
        if num_ops:
            self.num_ops = int(num_ops)
        else:
            self.num_ops = 10 * self.num_keys

        self.percentage_reads = decimal.Decimal(arguments[_PERCENTAGE_READS_FLAG])
        self.max_read_distance = decimal.Decimal(arguments[_MAX_READ_DISTANCE_FLAG])

    def _open_output_file(self):
        self._output_file = open(self.output_path, 'w')

    def _close_output_file(self):
        self._output_file.close()

    def get_output_file(self):
        if not self._output_file:
            self._open_output_file()

        return self._output_file

    def flush_output_file(self):
        if not self._output_file:
            return

        self._close_output_file()
        self._output_file = None


_KEY_TEMPLATE = 'key{}'

######################################################
# Main logic
######################################################

def write_output(workload_parameters: Parameters, output: str):
    workload_parameters.get_output_file().write('{}\n'.format(output))


def generate_command_kind_uniform(pool: List[CommandKind]) -> CommandKind:
    return pool[int(len(pool) * random.random())]


def _calculate_probability_list(
    pool: List[CommandKind], distribution: Dict[CommandKind, decimal.Decimal],
) -> List[float]:
    n_specified_values = len(distribution)
    specified_values = sum(distribution.values())

    for c in pool:
        if c in distribution:
            probability_list.append(float(distribution[c]))
            continue

        command_probability = (
            (decimal.Decimal('1.0') - specified_values) / (len(pool) - n_specified_values)
        )
        probability_list.append(float(command_probability))

    return probability_list


def generate_command_kind_distribution(
    pool: List[CommandKind], distribution: Dict[CommandKind, decimal.Decimal],
) -> CommandKind:
    probability_list = _calculate_probability_list(pool, distribution)

    return random.choices(pool, probability_list)[0]


def generate_command_kind_list_distribution(
    pool: List[CommandKind], distribution: Dict[CommandKind, decimal.Decimal], size: int,
) -> List[CommandKind]:
    probability_list = _calculate_probability_list(pool, distribution)

    return random.choices(pool, probability_list, k=size)


def generate_command_kind(
    exclude_reads: bool = False, distribution: Optional[Dict[CommandKind, decimal.Decimal]] = None,
):
    pool = [c for c in CommandKind if (c is not CommandKind.READ or not exclude_reads)]

    if distribution:
        return generate_command_kind_distribution(pool, distribution)

    return generate_command_kind_uniform(pool)


def generate_command_kind_list(
    size: int, distribution: Optional[Dict[CommandKind, decimal.Decimal]],
) -> List[CommandKind]:
    pool = [c for c in CommandKind]

    return generate_command_kind_distribution(pool, distribution, size)


def generate_key_uniform(workload_parameters: Parameters) -> str:
    rnd = int(workload_parameters.num_keys * random.random())

    return _KEY_TEMPLATE.format(rnd)


def generate_command_value() -> str:
    return 'value{}'.format(random.randint(0, 1_000_000_000))


def generate_preamble(workload_parameters: Parameters) -> Set[str]:
    """Seed function for the database. A preamble will contain only write operations
    for ~50% of the specified number of keys, and will return the set of keys
    that have their values set.
    """
    preamble_keys = set()
    for i in range(workload_parameters.num_keys // 2):
        while True:
            key = generate_key_uniform(workload_parameters)
            if key not in preamble_keys:
                break

        command = Command(CommandKind.WRITE, key, generate_command_value())
        write_output(workload_parameters, command.to_str())
        preamble_keys.add(key)

    return preamble_keys


def generate_workload(workload_parameters: Parameters):
    # set of keys that are present at any given point in the db
    preamble_keys = generate_preamble(workload_parameters)
    write_output(workload_parameters, 'end preamble')

    valid_read_keys = preamble_keys
    written_keys_and_locations = {k: 0 for k in preamble_keys}

    n_commands_written = 0
    n_commands_reads = 0

    target_percentage_reads = workload_parameters.percentage_reads
    max_read_distance = workload_parameters.max_read_distance
    distribution = {CommandKind.READ: target_percentage_reads}

    # NOTE consider replacing this whole thing with a call to
    # `generate_command_kind_list()` and then just iterating over that
    # and generating the keys. Downside: since there is no way to build it
    # using a generator, it will be terrible for large values of `num_keys`.
    last_written_key = next(iter(preamble_keys))
    while n_commands_written < workload_parameters.num_ops:
        while True:
            command_kind = generate_command_kind()

            command_value = None

            command_key = None
            if command_kind is not CommandKind.READ:
                command_key = generate_key_uniform(workload_parameters)

            if command_kind is CommandKind.WRITE:
                command_value = generate_command_value()
                written_keys_and_locations[command_key] = n_commands_written + 1
                valid_read_keys.add(command_key)
                last_written_key = command_key

                break
            elif command_kind is CommandKind.DELETE:
                if command_key in written_keys_and_locations:
                    del written_keys_and_locations[command_key]
                    valid_read_keys.remove(command_key)

                    break
            else:
                if (
                    command_key in written_keys_and_locations and
                    written_keys_and_locations[command_key] + max_read_distance >= n_commands_written + 1
                ):
                    n_commands_reads += 1
                    break

        command = Command(command_kind, command_key, command_value)
        write_output(workload_parameters, command.to_str())

        n_commands_written += 1

        n_retries = 0
        # NOTE ugly, but ok for now. This is not the best calculation really,
        # but it gets us near to where we want to be
        while (
            n_commands_written < workload_parameters.num_ops and
            decimal.Decimal(n_commands_reads) / decimal.Decimal(n_commands_written) <= target_percentage_reads
        ):
            n_retries += 1
            read_key = generate_key_uniform(workload_parameters)
            if (
                read_key not in written_keys_and_locations or
                written_keys_and_locations[read_key] + max_read_distance < n_commands_written + 1
            ):
                if n_retries < 5:
                    continue

                read_key = last_written_key

            command = Command(CommandKind.READ, read_key)
            write_output(workload_parameters, command.to_str())

            n_commands_written += 1
            n_commands_reads += 1

    # NOTE @dbg statistics
    # read_ratio = decimal.Decimal(n_commands_reads) / decimal.Decimal(n_commands_written)
    # print('Final distribution of commands: {0:.2f}% reads'.format(read_ratio))

def main(arguments):
    workload_parameters = Parameters()
    workload_parameters.populate_from_arguments(arguments)

    print(
        f'Generating a workload with {workload_parameters.num_keys} keys and'
        f' {workload_parameters.num_ops} operations'
    )
    write_output(workload_parameters, 'operation | key | value')
    generate_workload(workload_parameters)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
