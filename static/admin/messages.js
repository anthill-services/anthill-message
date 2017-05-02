(function(div, context)
{
    var controller = {
        ws: new ServiceJsonRPC(SERVICE, "stream_messages", context),
        init: function(div, context)
        {
            var zis = this;

            this.account = context["account"];
            this.messages = {};

            this.ws.handle("message", function(payload)
            {
                $('.messages-notice').remove();

                var id = payload["message_id"].slice(-8);

                var item = $('<li role="presentation"></li>').appendTo(zis.messages_list);

                var node = $('<a href="#"><i class="fa fa-envelope-o" aria-hidden="true"></i> ' +
                    id + '</a>').appendTo(item).click(function()
                {
                    zis.select_message(payload);
                    return false;
                });

                zis.messages[id] = payload;

                notify_success("New message received!");

                return true;
            });

            this.panel = $('<div class="panel panel-default"></div>').appendTo(div);
            this.header = $('<div class="panel-heading">' +
                  '<div class="row">' +
                      '<div class="col-sm-6">' +
                          '<h3 class="panel-title padFix"><i class="fa fa-refresh fa-spin" aria-hidden="true"></i> Received messages</h3>' +
                      '</div>' +
                  '</div>' +
              '</div>').appendTo(this.panel);

            this.body = $('<div class="panel-body"><div class="messages-notice">' +
                'Messages will appear here as they received.</div></div>').appendTo(this.panel);
            this.messages_list = $('<ul class="nav nav-pills"></ul>').appendTo(this.body);

            this.tabs_header = $('<ul class="nav nav-tabs" data-tabs="tabs">' +
                '<li class="active"><a href="#server_status" id="server_status_header" data-toggle="tab"></a></li>' +
                '<li><a href="#send_message" id="send_message_header" data-toggle="tab">' +
                    '<i class="fa fa-pencil" aria-hidden="true"></i> Send message</a></li>' +
                '</ul>').appendTo(div);
            this.tabs_content = $('<div class="tab-content">' +
                '<div class="tab-pane active" id="server_status"></div>' +
                '</div>').appendTo(div);

            var send_message = $('<div class="tab-pane" id="send_message"></div>').appendTo(this.tabs_content);

            render_node({
                "class": "form",
                "context": {},
                "methods": {
                    "post": {"style": "primary", "title": "Send"}
                },
                "fields": {
                    "recipient_class": {"style": "primary", "validation": "non-empty", "type": "text", "value": "user",
                        "title": "Recipient Class", "order": 1
                    },
                    "recipient_key": {"style": "primary", "validation": "non-empty", "type": "text", "value": null,
                        "title": "Recipient Key", "order": 1
                    },
                    "sender": {"style": "primary", "validation": "number", "type": "text", "value": this.account,
                        "title": "From", "order": 2
                    },
                    "message_type": {"style": "primary", "validation": "non-empty", "type": "text", "value": "debug",
                        "title": "Type", "order": 3
                    },
                    "message": {"style": "primary", "validation": "non-empty", "type": "json", "value": {},
                        "title": "Message", "order": 4, "height": 200
                    },
                    "flags": {"style": "primary", "validation": "non-empty", "type": "dorn",
                        "value": ["remove_delivered"],
                        "title": "Flags", "order": 5, "schema": {
                            "type": "array",
                            "uniqueItems": true,
                            "title": "Other Flags",
                            "items": {
                                "type": "string",
                                "enum": ["remove_delivered"],
                                "options": {
                                    "enum_titles": ["Delete once delivered"]
                                }
                            }
                        }
                    }
                },
                "title": "Send a message",
                "callback": function(fields)
                {
                    try 
                    {
                        JSON.parse(fields["message"]);
                    }
                    catch (e)
                    {
                        notify_error(e);
                        return false;
                    }

                    var flags = JSON.parse(fields["flags"]);

                    zis.ws.request("send_message", {
                        "recipient_class": fields["recipient_class"],
                        "recipient_key": fields["recipient_key"],
                        "sender": fields["sender"],
                        "message_type": fields["message_type"],
                        "message": fields["message"],
                        "flags": flags
                    }).done(function(payload)
                    {
                        notify_success("Message sent!");
                    }).fail(function(code, message, data)
                    {
                        notify_error("Error " + code + ": " + message);
                    });

                    return false;
                }
            }, send_message);

            this.status('Connecting...', 'refresh', 'info');

            this.ws.onopen = function()
            {
                zis.status('Connected', 'check', 'success');
            };

            this.ws.onclose = function (code, reaspon)
            {
                zis.status('Error ' + code + ": " + reaspon, 'times', 'danger');
            };
        },
        render_values: function (to, kv)
        {
            to.html('');
            var table = $('<table class="table"></table>').appendTo(to);

            for (var key in kv)
            {
                var value_obj = kv[key];

                var decorators = {
                    "label": function(value, agrs)
                    {
                        return $('<span class="label label-' + agrs.color + '">' + value + '</span>');
                    },
                    "json": function(value, agrs)
                    {
                        return new JSONFormatter(value, 0).render();
                    },
                    "icon": function (value, args)
                    {
                        var node = $('<span></span>');

                        node.append('<i class="fa fa-' + args.icon + '" aria-hidden="true"></i> ' +
                            value);

                        return node;
                    }
                };

                var tr = $('<tr></tr>').appendTo(table);
                var property = $('<td class="col-sm-1 th-notop">' + value_obj.title + '</td>').appendTo(tr);
                var value = $('<td class="col-sm-3 th-notop"></td>').appendTo(tr);

                if (value_obj.decorator != null)
                {
                    var d = decorators[value_obj.decorator];

                    if (d != null)
                    {
                        value.append(d(value_obj.value, value_obj.args))
                    }
                }
                else
                {
                    value.append(value_obj.value);
                }
            }
        },
        select_message: function(message)
        {
            var zis = this;

            var id = message["message_id"].slice(-8);
            var s = this.messages[id];

            if (s.tab_header == null)
            {
                s.tab_header = $('<li><a href="#message_' + id + '" data-toggle="tab">' + id + '</a></li>').
                    appendTo(this.tabs_header);
                s.tab_content = $('<div class="tab-pane" id="message_' + id + '"></div>').appendTo(this.tabs_content);
                s.tab_properties = $('<div></div>').appendTo(s.tab_content);
            }

            s.tab_header.find('a').tab('show');

            this.render_values(s.tab_properties, [
                {
                    "title": "Message UUID",
                    "value": message["message_id"]
                },
                {
                    "title": "Sender",
                    "value": s["sender"],
                    "decorator": "label",
                    "args": {
                        "color": "info"
                    }
                },
                {
                    "title": "Message Type",
                    "value": s["message_type"],
                    "decorator": "label",
                    "args": {
                        "color": "danger"
                    }
                },
                {
                    "title": "Payload",
                    "value": s["payload"],
                    "decorator": "json"
                }
            ]);
        },
        status: function (title, icon, color)
        {
            var server_status_header = $('#server_status_header');
            var server_status = $('#server_status');

            server_status_header.html(
                '<i class="fa fa-' + icon + ' text-' + color + '" aria-hidden="true"></i>' +
                ' Server status');

            this.render_values(server_status, [
                {
                    "title": "Connection status",
                    "value": title,
                    "decorator": "label",
                    "args": {
                        "color": color
                    }
                }
            ]);
        }
    };

    controller.init(div, context);
});
