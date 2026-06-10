// Channel self-registration barrel.
// Each import triggers the channel module's registerChannelAdapter() call.
//
// Main ships with one default channel — `cli`, the always-on local-terminal
// channel. Other channel skills (/add-slack, /add-discord, /add-whatsapp,
// ...) copy their module from the `channels` branch and append a
// self-registration import below.
//
// DO NOT re-add iMessage. It was removed intentionally on 2026-05-17 —
// approval routing is consolidated on Discord, and iMessage was creating
// noise (unregistered senders, dropped messages, dupe owner roles).

import './cli.js';
import './discord.js';
