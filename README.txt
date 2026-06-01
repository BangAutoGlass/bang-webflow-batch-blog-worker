Bang Webflow Batch Blog Worker - Body/Meta Fixed

This worker matches the batch edge function that publishes confirmed Webflow Blog Posts fields.

Confirmed Webflow API slugs:
- Post Body: rich-text
- Meta Title: meta-title
- Meta Description: meta-description

The worker still strips unsupported generated-only fields before Webflow publish:
- post-body
- post-body-2
- meta_title
- meta_description
- faqHtml
- ctaText
- imagePrompt
- notes
- tags

Deploy this folder as the Render worker repo.
