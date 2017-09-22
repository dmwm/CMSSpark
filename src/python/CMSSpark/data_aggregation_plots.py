import matplotlib.pyplot as plt
import csv
import time
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"

        self.parser = argparse.ArgumentParser(prog='PROG', description='')

        self.parser.add_argument("--input_filename", action="store",
            dest='input_filename', default='', help='Input filename or path including filename')


# Open a file and read everything into dictionaries according to first line (header) of file
# For example if header is A,B and file contains entries 1,2 and 3,4 then the output will be
# Array with such entries: [{"A":1, "B":2"}, {"A":3, "B":"4"}]
# This piece of code also finds "timestamp" attribute (UNIX time milliseconds),
# converts it into YYYYMMDD date and saves as date attribute.
def read_file(input_filename=''):
    rows = []
    with open(input_filename, 'r') as csvfile:
        plots = csv.reader(csvfile, delimiter=',')
        header = next(csvfile, None).strip().split(',')
        # site_name, dataset_name, nacc, distinct_users, stream, timestamp, site_tier, cpu_time, primary_name, processing_name, data_tier

        print("CSV header " + str(header))

        header_length = len(header)

        for row in plots:
            row_values = {}
            # print(str(row))
            for i in range(0, header_length):
                row_values[header[i]] = row[i]

            if 'timestamp' in row_values:
                row_values['date'] = time.strftime("%Y-%m-%d", time.gmtime(int(row_values ['timestamp']) / 1000))
            rows.append(row_values)
        print("Found " + str(len(rows)) + " records in file " + str(input_filename))
    return rows


# Plot number of access (sum of 'nacc') for every date ('date')
# If no output_filename is specified, output is shown on screen. Otherwise it is plotted into file.
def number_of_access(rows, output_filename=None):
    values = {}

    for row in rows:
        key = row['date']
        value = int(row['nacc'])
        if key in values:
            values[key]=values[key]+value
        else:
            values[key]=value

    x=[]
    y=[]
    sum=0

    for t in sorted(values):
        x.append(t)
        y.append(values[t])
        sum=sum+values[t]
        # print(t + " -> " + str(values[t]))

    print("Total number of accesses is " + str(sum))

    plt.bar(range(len(x)),y, label='')
    plt.xticks(range(len(x)), [label[5:] for label in x])
    plt.xlabel('Date')
    plt.ylabel('Number of access')
    ax = plt.gca()
    ax.get_yaxis().get_major_formatter().set_scientific(False)
    plt.xticks(rotation=45)
    if output_filename == None:
        plt.show()
    else:
        figure = plt.gcf()
        # Set figure size in inches
        figure.set_size_inches(8, 6)
        plt.savefig(output_filename, dpi=100)
    plt.close()


# Groups 'entries' by 'columns' into 'buckets'. For example if entries have "A" and "B" attributes, all entries will
# be placed (as arrays) into dictionaries with equal values of these attributes. Dictionary key will be value of that
# attribute.
# For example such records: [{"A":"1", "B":"2"}, {"A":"3", "B":"4"}, {"A":"1", B:"4"}, {"A":"1", B:"2"}]
# grouped by "A" and "B" would look like this:
# {"1":{"2":[{"A":"1", "B":"2"}, {"A":"1", "B":"2"}], "4":[{"A":"1", "B":"4"}]}, "3":{"4":[{"A":"3", "B":"4"}]}}
# Note that order of keys in columns array is important!
# If sumColumn is not equal to None then arrays will contain sums of this attribute rather than entries themselves.
def make_buckets(columns, entries, sum_column=None):
    buckets = {}

    column = columns[0]
    last = len(columns) == 1

    for row in entries:
        key = row[column]

        if key not in buckets:
            buckets[key] = []

        buckets[key].append(row)

    if not last:
        result_buckets = {}
        for bucket_key in buckets:
            result_buckets[bucket_key] = make_buckets(columns[1:], buckets[bucket_key], sum_column)
        return result_buckets
    else:
        if sum_column == None:
            return buckets
        else:
            return sum_array(buckets, sum_column)


# Return sum of certain attribute in 'bucket'
# Bucket structure is as follows {"1":[{"B":1, "C":5}, {"B":2}], "2":[{"B":3}, {"B":4, "C":6}]}
# Result would be {'1":3, "2":7} for column "B"
def sum_array(buckets, column):
    result_buckets = {}
    for bucket_key in buckets:
        sum = 0
        bucket = buckets[bucket_key]
        for entry in bucket:
            entry_value = float(entry[column])
            if entry_value > 0:
                sum = sum + entry_value
        result_buckets[bucket_key] = int(sum)

    return result_buckets


# Expects a following structure:
# {"A":{"a":1, "b":2}, B:{"b":5, "c"6}}
# Uses outer key ("A", "B") to draw separate lines. Uses inner key ("a", "b", "c") as x axis (preferably date)
# Outputs
def draw_buckets(buckets, top_results=5, filename=None):
    x = []
    for primary_bucket in buckets:
        bucket = buckets[primary_bucket]

        for key in bucket:
            if key not in x:
                x.append(key)

    x = sorted(x)

    ys = {}
    for primary_bucket in buckets:
        bucket = buckets[primary_bucket]

        y = []
        for key in x:
            if key in bucket:
                y.append(bucket[key])
            else:
                y.append(0)

        ys[primary_bucket] = y

    max_ys = {}

    print('Will draw only ' + str(top_results) + ' results')

    for i in range(0, top_results):
        max = 0
        max_key = None
        for y_key in ys:
            if y_key not in max_ys:
                sum_of_values = sum(ys[y_key])
                if sum_of_values > max or max_key == None:
                    max = sum_of_values
                    max_key = y_key

        if max_key != None:
            max_ys[max_key]=ys[max_key]

    for y_key in max_ys:
        plt.plot(range(len(x)), max_ys[y_key], label=y_key)

    plt.legend()
    plt.xticks(range(len(x)), [label[5:] for label in x])
    plt.xlabel('Date')
    plt.ylabel('Number of access')
    ax = plt.gca()
    ax.get_yaxis().get_major_formatter().set_scientific(False)
    plt.xticks(rotation=45)
    if filename == None:
        plt.show()
    else:
        figure = plt.gcf()
        # Set figure size in inches
        figure.set_size_inches(8, 6)
        plt.savefig(filename, dpi=100)
    plt.close()


# Output entries as a csv table.
# title1 and title 2 - column titles
def make_table(bucket, title1, title2, limit_results=None, filename=None):
    sorted_bucket_keys = sorted(bucket, key=bucket.get, reverse=True)
    sum = 0
    for key in sorted_bucket_keys:
        sum += int(bucket[key])

    if limit_results != None:
        sorted_bucket_keys = sorted_bucket_keys[0:limit_results]

    sum_of_table = 0
    csv = '"' + title1 + '","' + title2 + '","Percentage of all"\n'
    for key in sorted_bucket_keys:
        sum_of_table += bucket[key]

        csv += '"' + key +'",'
        csv += '"' + str((bucket[key])) + '",'
        csv += '"' + "{0:.4f}".format(100.0 * bucket[key] / sum) + '"'
        csv += '\n'

    csv += '"Sum","' + str(sum_of_table) + '","' + "{0:.4f}".format(100.0 * sum_of_table / sum) + '"'
    if filename != None:
        csv_file = open(filename, 'w')
        csv_file.write(csv)
        csv_file.close()
    else:
        print (csv)


# If record's value of 'column' is not in 'valid_values', change it to 'other_value'
def filter_values(records, column, valid_values, other_value):
    new_records = list(records)

    for record in records:
        if record[column] not in valid_values:
            record[column] = other_value

    return new_records


# If record's value of 'column' is not in 'valid_values', omit it from results
def omit_values(records, column, valid_values):
    new_records = []

    for record in records:
        if record[column] in valid_values:
            new_records.append(record)

    return new_records


def run(input_file_name):
    rows = read_file(input_file_name)
    # Include only T0, T1, T2, T3 site tiers. Set others to 'Other'
    rows = filter_values(rows, 'site_tier', ['T0', 'T1', 'T2', 'T2', 'T3'], 'Other')

    rows_aaa = omit_values(rows, 'stream', ['aaa'])
    rows_cmssw = omit_values(rows, 'stream', ['cmssw'])
    rows_eos = omit_values(rows, 'stream', ['eos'])
    rows_jm = omit_values(rows, 'stream', ['jm'])

    number_of_access(rows, 'NumberOfAccess.png')

    grouped_by_tier = make_buckets(["data_tier"], rows, "nacc")
    grouped_by_site_tier = make_buckets(["site_tier"], rows, "nacc")
    grouped_by_date_and_tier = make_buckets(["data_tier", "date"], rows, "nacc")
    grouped_by_date_and_site_tier = make_buckets(["site_tier", "date"], rows, "nacc")

    grouped_by_dataset_eos = make_buckets(["dataset_name"], rows_eos, "nacc")

    draw_buckets(grouped_by_date_and_tier, 10, "GroupedByDateAndTier.png")
    draw_buckets(grouped_by_date_and_site_tier, 10, "GroupedByDateAndSiteTier.png")
    make_table(grouped_by_tier, "Tier", "Number of accesses", 20, 'GroupedByTier.csv')
    make_table(grouped_by_site_tier, "Site tier", "Number of accesses", 20, 'GroupedBySiteTier.csv')

    make_table(grouped_by_dataset_eos, "Dataset", "Number of accesses", 5, "GroupedByDatasetEOS.csv")


# Main function
def main():
    start_time = time.time()

    option_parser = OptionParser()
    options = option_parser.parser.parse_args()

    print("Input arguments: %s" % options)
    run(options.input_filename)

    end_time = time.time()
    print('Start time         : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(start_time)))
    print('End time           : %s' % time.strftime('%Y-%m-%d %H:%M:%S GMT', time.gmtime(end_time)))
    print('Total elapsed time : %s' % (end_time - start_time))


if __name__ == '__main__':
    main()
