"""Coordinator-local terminal frame styling."""

# This static stylesheet is package-owned so the terminal UI has no application dependency.
CSS = """
Screen, .framed { background: #f7f7f3; color: #111111; }
Header { display: none; }
Footer { background: #e3e3df; color: #111111; height: 1; }
Footer > .footer-key--key { background: #e3e3df; color: #111111; text-style: bold; }
Footer > .footer-key--description { background: #e3e3df; color: #333333; }
DataTable { background: #ffffff; color: #111111; border: none; }
DataTable > .datatable--header { background: #ddddda; color: #111111; text-style: bold; }
DataTable > .datatable--cursor { background: #111111; color: #ffffff; text-style: bold; }
Button { background: #e0e0dd; color: #111111; border: none; height: 1; margin: 0 1 0 0; }
Button.-primary { background: #111111; color: #ffffff; }
Button:disabled { color: #777777; }

.subject { height: auto; padding: 0 1; background: #111111; color: #ffffff;
           text-style: bold; }
.detail-rule { height: 1; padding: 0 1; background: #f7f7f3; color: #333333; }
.detail { height: auto; padding: 0 1; background: #ffffff; color: #111111; }
.status { height: 1; padding: 0 1; background: #e3e3df; color: #222222; }

#show, #confirm { width: 76%; max-width: 96; height: auto; max-height: 90%;
                  padding: 0 1; background: #ffffff; color: #111111;
                  border: double #111111; }
#show.wide { width: 96%; max-width: 150; max-height: 94%; }
#show-title, #confirm-title { height: auto; padding: 0 1; background: #ddddda;
                              color: #111111; text-style: bold; }
#show-scroll { height: auto; max-height: 100%; padding: 0 1; overflow-y: auto;
               background: #ffffff; color: #111111; }
#show-body { height: auto; background: #ffffff; color: #111111; }
#show-keys { height: 1; }
#confirm-effects { height: auto; padding: 0 1; color: #222222; }
#confirm-buttons { height: auto; }
"""
