# package install
import re

# Extract source tables in DDL and put into list
def source_tables_from(ddl):
    """
    Extract text after 'database.' at each occurance and stop at the following space.
    Return the source table name
    """
    # remove the /* */ comments and convert all to lower
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", ddl).lower()
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)
    result = list()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.append(tok)
                remove_items = ["to_date", "sysdate"]
                result2 = [x for x in result if x not in remove_items]
            get_next = False
        get_next = tok.lower() in ["from", "join"]
    return result2


# Extract datalake tables in DDL and put into list
def tables_from(ddl):
    """
    Extract text after 'bronze.' / 'silver.' / 'gold.' at each occurrence and stop at the following space
    Return the following table name
    """
    # remove the /* */ comments and convert all to lower
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", ddl).lower()
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)
    result = list()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.append(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]
    return list(set([x for x in result if 'bronze.' in x or 'silver.' in x or 'gold.' in x]))
