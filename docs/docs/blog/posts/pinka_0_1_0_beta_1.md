---
slug: v0_1_0_beta_1_release
date: 2025-02-24T00:00:00Z
---


# Pinka 0.1.0-beta.1 is Released!

We are thrilled to announce the release of Pinka 0.1.0-beta.1! This marks a significant milestone as we move closer to a stable release. Pinka is now ready for testing integration with static site generators, making it easier than ever to add federated commenting to your static websites.

## What's New?

In this beta release, we've focused on enhancing the robustness and reliability of Pinka. Here are some of the key features and improvements:

- **ActivityPub Integration**: Seamless federation with Mastodon and other Fediverse platforms.
- **Enhanced Delivery Mechanism**: More robust retry mechanisms for failed deliveries.
- **Improved Error Reporting**: Better logging and error handling to help diagnose issues quickly.
- **Basic Comments Rendering**: Initial implementation of comments rendering for HTTP.

## Getting Started

To get started with Pinka, you can use our prebuilt container:

```bash
git clone https://github.com/pinka-social/pinka
docker run --rm --security-opt label=disable \
        -v ./pinka/examples:/etc/pinka \
        quay.io/pinka/pinka \
        run -c /etc/pinka/config-single.toml
```

For detailed documentation, please visit our [website](https://pinka.dev).

## Feedback and Contributions

We welcome your feedback and contributions! Please report any issues you encounter and feel free to contribute to the project. Check out our [CONTRIBUTING.md](https://github.com/pinka-social/pinka/blob/main/CONTRIBUTING.md) for guidelines on how to get involved.

You can find the source code and contribute to the project on our [GitHub repository](https://github.com/pinka-social/pinka).

## Call for Contributions

We are actively seeking contributions in the following areas:

- **Website Development**: Help us improve our website and documentation.
- **Design**: Contribute to the design of our user interface and user experience.
- **Documentation**: Assist in writing and refining our documentation to make it more comprehensive.
- **Feature Ideation**: Share your ideas for new features and enhancements.

Thank you for your support, and we look forward to your feedback on this beta release!

Happy commenting!