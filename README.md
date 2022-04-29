# mbox-indexer

The [MBOX format](https://www.loc.gov/preservation/digital/formats/fdd/fdd000383.shtml) is a text format for storing
multiple email messages. It's the preferred import/export format for many email services and clients, including the one
that rhymes with bugle.

A message begins with `From `. The next message begins with the same string at the start of a line. An mbox file can be
huge (gigabytes) so we don't want to load it all into memory. This project's goal is to create an index for full-text
search of an mbox file without altering the mbox file itself.

## License

Copyright 2022 Andrew Morrow. All rights reserved.

This program is licensed to you under the terms of the Parity public license (see LICENSE.md).