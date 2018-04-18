﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DotNetKafkaExample
{
    // 送信メッセージ
    class SendMessage
    {
        public string Message { get; set; }

        public long Timestamp { get; set; }

    }

    // 受信メッセージ
    class ConsumedMessage
    {
        public string Message { get; set; }

        public long Timestamp { get; set; }

    }
}
