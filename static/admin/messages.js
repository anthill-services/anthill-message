(function(div, context)
{
    var controller = {
        ws: new ServiceJsonRPC(SERVICE, "stream_messages", context),
        init: function(div, context)
        {
            var zis = this;

            this.account = context["account"];

            this.ws.handle("message", function(payload)
            {
                notify_success("New message received!")
            });

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
                    "recipient_key": {"style": "primary", "validation": "number", "type": "text", "value": null,
                        "title": "Recipient Key", "order": 1
                    },
                    "sender": {"style": "primary", "validation": "number", "type": "text", "value": this.account,
                        "title": "From", "order": 2
                    },
                    "message": {"style": "primary", "validation": "non-empty", "type": "json", "value": {},
                        "title": "Message", "order": 3, "height": 200
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

                    zis.ws.request("send_message", {
                        "recipient_class": fields["recipient_class"],
                        "recipient_key": fields["recipient_key"],
                        "sender": fields["sender"],
                        "message": fields["message"]
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
