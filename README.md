# Pinka 🦋

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

A highly available, ActivityPub-powered commenting system that bridges the gap
between web comments and the Fediverse. Perfect for blogs looking to engage with
Mastodon and other ActivityPub-compatible platforms.

> The name "Pinka" comes from the [Lojban
> word](https://en.wiktionary.org/wiki/Appendix:Lojban/pinka) meaning "to
> comment" (x₁ is a comment/remark about subject x₂ expressed by x₃ to audience
> x₄). This reflects both the project's core purpose and its systematic approach
> to federated communications.

> [!WARNING]
> Pinka is still under active development. The database schema might change between
> minor versions. See [FEATURES.md](FEATURES.md) for currently implemented features.

> [!NOTE]
> Federating with Mastodon mostly works now!

## ✨ Features

- 🔄 **Full ActivityPub Integration**
  - Actor profile, inbox, and outbox support
  - Seamless federation with Mastodon and other Fediverse platforms

- 🎯 **Easy Integration**
  - RESTful API for comment management
  - Drop-in web components for instant commenting functionality

- 🎨 **Highly Customizable**
  - Flexible Tera templating system
  - Fully customizable via CSS

- 🔒 **Built for Reliability**
  - Raft-based clustering powered by Ractor actor system
  - Self-healing data replication
  - Robust comment moderation system

- 📦 **Zero External Dependencies**
  - Powered by Fjall, an embedded key/value database
  - No external database setup or management required
  - Self-contained and easy to deploy

## 💡 Why Pinka?

Pinka is designed to be **simple to deploy** and **easy to maintain**. With its embedded Fjall database and Ractor-based clustering, you don't need to worry about:
- Setting up external databases
- Complex deployment procedures
- Additional infrastructure costs
- Cluster coordination complexity

Just deploy and run!

## 🚀 Quick Start

```bash
# Installation instructions here
🚧
```

## 📖 Documentation

For detailed documentation, please visit our website (🚧).

## 🛠️ Integration Example

```
<pinka-comments
  site="your-site-url"
  page-id="unique-page-id">
</pinka-comments>
```

## 🤝 Contributing

We welcome contributions! By contributing, you agree to license your work under MIT OR Apache-2.0 license.

Please read our CONTRIBUTING.md for:
- Development setup
- Coding guidelines
- DCO requirements
- Pull request process
- Community guidelines

## 📜 License

This project is licensed under either of:

* [MIT License]()
* [Apache License 2.0]()

at your option.

## 🌟 Acknowledgments

Pinka stands on the shoulders of giants. Special thanks to:

* [Fjall](https://github.com/fjall-rs/fjall) - The robust embedded key/value database that powers our storage layer
* [Ractor](https://github.com/slawlor/ractor) - The excellent actor system that enables our reliable clustering capabilities

Thanks to all contributors who help make Pinka better!
