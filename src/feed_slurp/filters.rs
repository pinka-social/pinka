use std::sync::LazyLock;

use unicode_segmentation::UnicodeSegmentation;

static AMMONIA: LazyLock<ammonia::Builder> = LazyLock::new(|| ammonia::Builder::empty());

/// Strip HTML tags from a string
pub(super) fn to_text(value: &str) -> String {
    AMMONIA.clean(value).to_string()
}

/// Truncate a string to a certain number of words.
/// Each CJK character is counted as one word. Last word is not truncated.
///
/// # Examples
///
/// ```
/// assert_eq!(excerpt("Lorem ipsum dolor sit amet", 1), "Lorem...");
/// assert_eq!(excerpt("Lorem ipsum dolor sit amet", 2), "Lorem ipsum...");
/// assert_eq!(excerpt("Lorem ipsum dolor sit amet", 3), "Lorem ipsum dolor...");
/// assert_eq!(excerpt("Lorem ipsum dolor sit amet", 4), "Lorem ipsum dolor sit...");
/// assert_eq!(excerpt("美校，背衣尾未", 4), "美校，背衣...");
/// ```
pub(super) fn excerpt(value: &str, words: u32) -> String {
    let mut word_count = 0;
    let mut result = String::new();

    for word in value.split_word_bounds() {
        if word_count >= words {
            break;
        }
        result.push_str(word);
        if !word
            .chars()
            .any(|c| c.is_whitespace() || c.is_ascii_punctuation())
        {
            word_count += 1;
        }
    }

    result.push_str("...");
    result
}

#[cfg(test)]
mod tests {
    use super::excerpt;

    #[test]
    fn excerpt_latin() {
        assert_eq!(excerpt("Lorem ipsum, dolor sit amet", 1), "Lorem...");
        assert_eq!(excerpt("Lorem ipsum, dolor sit amet", 2), "Lorem ipsum...");
        assert_eq!(
            excerpt("Lorem ipsum, dolor sit amet", 3),
            "Lorem ipsum, dolor..."
        );
        assert_eq!(
            excerpt("Lorem ipsum, dolor sit amet", 4),
            "Lorem ipsum, dolor sit..."
        );
    }

    #[test]
    fn excerpt_cjk() {
        assert_eq!(excerpt("美校，背衣尾未", 4), "美校，背...");
    }
}
