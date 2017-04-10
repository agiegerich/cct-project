import re
pattern = re.compile('^\$\$\$===cs5630s17===\$\$\$===((Title)|(cs5630s17))===\$\$\$.*')


print(pattern.match('$$$===cs5630s17===$$$===Title===$$$'))

