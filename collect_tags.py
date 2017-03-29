from __future__ import print_function
from optparse import OptionParser
from collections import defaultdict
from time import time
import contextlib
import sqlite3

def get_options():
    parser = OptionParser()
    parser.add_option(
        '-l', '--labels',
        dest='labels',
        help="Image labels (expect labels.csv file)", metavar="FILENAME"
    )
    parser.add_option(
        '-d', '--dictionary',
        dest='dict',
        help="Label dictionary (expect dict.csv file)", metavar="FILENAME"
    )
    parser.add_option(
        '-m', '--metadata',
        dest='meta',
        help="Image metadata (expect images.csv)", metavar="FILENAME"
    )
    parser.add_option(
        '-o', '--outfile',
        dest='outfile',
        help="Output filename", metavar="FILENAME"
    )
    options, args = parser.parse_args()
    if not all([options.labels, options.dict, options.meta, options.outfile]):
        parser.print_help()
        exit(1)
    return options

def collect_labels(labels_file):
    labels = defaultdict(set)
    with open(labels_file, 'r') as f:
        next(f) # skip first line (expect column headings)
        for line in f:
            image_id, _, label, confidence = line.rstrip().split(',')
            # print(image, tag, confidence)
            if float(confidence) > 0.5:
                labels[label].add(image_id)
    return labels

@contextlib.contextmanager
def load_tags(path):
    tags = {}
    with open(path, 'r') as f:
        for line in f:
            tag, value = line.rstrip().split('","')
            tag, value = tag[1:], value[:-1] # remove double quotes
            tags[tag] = value
    yield tags
    tags.clear()

@contextlib.contextmanager
def load_urls(path):
    urls = {}
    with open(path, 'r') as f:
        next(f) # skip first line (expect column headings)
        for line in f:
            fields = line.rstrip().split(',')
            image_id, url = fields[0], fields[2]
            urls[image_id] = url
    yield urls
    urls.clear()

def translate_ids(labels, dict_file, meta_file):
    decoded = {}
    with load_tags(dict_file) as tags, load_urls(meta_file) as urls:
        for tag, images in labels.iteritems():
            desc = tags[tag]
            image_urls = set(urls[image_id] for image_id in images)
            decoded[desc] = image_urls
    return decoded

def write_to_file(tag_images, db_name):
    conn = sqlite3.connect(db_name)
    conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")
    c = conn.cursor()
    c.execute(
        "DROP TABLE IF EXISTS tags"
    )
    c.execute(
        "CREATE TABLE tags (tag text PRIMARY KEY, urls text)"
    )
    for tag, images in tag_images.iteritems():
        c.execute(
            "INSERT INTO tags VALUES (?, ?)", (tag, ",".join(images))
        )
    conn.commit()
    conn.close()

def main():
    start_time = time()
    options = get_options()
    print("Collecting tags...")
    tag_images = collect_labels(options.labels)
    print("Translating ids...")
    tag_images = translate_ids(tag_images, options.dict, options.meta)
    print("Writing to file...")
    write_to_file(tag_images, options.outfile)
    print("Complete! Total execution time: {} seconds".format(time() - start_time))

if __name__=='__main__':
    main()
    
