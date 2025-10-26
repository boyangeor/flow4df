# import flow4df
from flow4df.misc.application_arguments import ApplicationArguments


def f1(application_arguments: ApplicationArguments) -> None:
    for at in application_arguments.active_tables:
        print(at.table_identifier.versionless_name)

    for t in application_arguments.tables:
        print(t.table_identifier.full_name)


def f2(application_arguments: ApplicationArguments) -> None:
    print('Processing active tables:')
    for at in application_arguments.active_tables:
        print('Processing:', at.table_identifier.versionless_name)
