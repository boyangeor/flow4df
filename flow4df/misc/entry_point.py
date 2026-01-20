"""Entry point for Spark applications."""
import sys
from flow4df.example_dwh import table_index
from flow4df.misc.application_arguments import ApplicationArguments


def main() -> None:
    application_args = ApplicationArguments.from_json(
        json_string=sys.argv[1], table_index=table_index
    )
    main_function = application_args.main
    main_function(application_args)
    return None


if __name__ == '__main__':
    main()
