import re
import logging as logger

logger.basicConfig(level=logger.DEBUG)

class HibernateLogParser:
    """
        Detects sql-queries and the associated binding parameters in a hibernate log file
        and constructs the resolved sql statements where the parameter placholder '?' is replaced 
        with the correct parameter value.  
    """

    start_of_select_statement_regex = '^[^\d].*select\s*$'
    param_log_entry_regex = '^.*org.hibernate.type.descriptor.sql.BasicBinder.*'
    resolved_sql_statements = []
    line_no = 0
    line = ''

    def __init__(self, lines=None, input=None):
        if lines:
            self.lines = [l.replace('\n','') for l in lines]
        elif input:
            self.lines = [l for l in self.input.split("\n") if l]
        else:
            raise ValueError("Either lines or input string must be given as parameter")

    def readline(self):
        if self.line_no < len(self.lines):
            self.line = self.lines[self.line_no]
            self.line_no = self.line_no + 1
        else:
            raise EOFError

    def parse_log_file(self):
        try: 
            while(True):
                logger.info("Start parsing")
                start, sql = self.extract_next_sql_query()
                logger.info(f"Detected sql statement at lines {start}-{self.line_no}")
                start, params = self.extract_parameter_values()
                logger.info(f"Detected binding block at lines {start}-{self.line_no}")
                resolved_sql = self.resolve_placeholders(sql, params)
                self.resolved_sql_statements.append(resolved_sql)
        except EOFError:
            logger.info("Reached end of input")

    def extract_next_sql_query(self):

        while(not re.match(self.start_of_select_statement_regex, self.line)):
            self.readline()

        start = self.line_no
        stmt = self.line + '\n'
        self.readline()
        while not re.match(self.param_log_entry_regex, self.line):
            stmt = stmt + self.line + '\n'
            self.readline()

        return (start, stmt);

    def extract_parameter_values(self):
        pattern = 'binding parameter \[\d+\] as \[(.*)\] - \[(.*)\]'
        params = []
        start = self.line_no
        while re.match(self.param_log_entry_regex, self.line):
            match = re.search(pattern, self.line)
            if match:
                type = match.group(1)
                value = match.group(2)
                value = "'" + value + "'" if type == 'VARCHAR' and not value == 'null' else value
                params.append(value)
            try:
                self.readline()
            except:
                return (start, params)
            
        return (start, params)

    def resolve_placeholders(self, sql, param_values):
        result = ''
        j = 0
        for c in sql:
            if (c == '?' and j < len(param_values)):
                result = result + param_values[j]
                j = j+1
            else:
                result = result + c
        return result


import sys

def main():
    """
        "Usage: >> python sql_log_assembler.py {filename}"
        Expects one filename parameter. 
        The output is written to the file {input_filename}.out. 
        Note that the output file will be overwritten by default.  
    """

    if not len(sys.argv) == 2:
        print("Usage: >> python sql_log_assembler.py {filename}")
        return

    filename = sys.argv[1]
    with open(filename) as file:
        lines = [line for line in file]

    p = HibernateLogParser(lines=lines)
    p.parse_log_file()

    filename = filename + '.out'
    with open(filename, 'w') as file:
        for sql in p.resolved_sql_statements:
            file.write(sql)

    print(f"Output written to {filename}")


if __name__ == "__main__":
    main()