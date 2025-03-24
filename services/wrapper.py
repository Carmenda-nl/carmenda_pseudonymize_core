import logging

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    logger_main = logging.getLogger(__name__)
    logger_main.setLevel(logging.INFO)

    handler_stream = logging.FileHandler('data/log/main.log')
    handler_stream.setFormatter(formatter)
    logger_main.addHandler(handler_stream)

    # this can help suppress extremely verbose logs
    logger_py4j = logging.getLogger('py4j')
    logger_py4j.setLevel('ERROR')  # default seems to be DEBUG, good alternative might be WARNING
    logger_py4j.addHandler(logging.FileHandler('data/log/py4j.log'))

    parser = argparse.ArgumentParser(
        description='Input parameters for the python program.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    def parse_dict_cols(mapping_str):
        return dict(item.strip().split('=') for item in mapping_str.split(','))

    def parse_list_cols(mapping_str):
        return [item.strip() for item in mapping_str.split(',')]

    args = parser.parse_args()
    args.input_cols = parse_dict_cols(args.input_cols)
    args.output_cols = parse_dict_cols(args.output_cols)

    if args.partition_n is not None:
        args.partition_n = int(args.partition_n)
    if args.coalesce_n is not None:
        args.coalesce_n = int(args.coalesce_n)

    args.max_n_processes = int(args.max_n_processes)
    parsed_args_str = '\n'.join(f' |-- {key}={value}' for key, value in vars(args).items())
    logger_main.info(f'Parsed arguments:\n{parsed_args_str}\n')
