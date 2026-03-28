# Methodology
# Save as: gui/report_engine/content/sections/methodology.md

Experiments were executed on {{system_profile.summary}}.
Energy measurements were collected via Linux RAPL (Running Average Power Limit)
sensors across the following power domains: {{system_profile.rapl_zones|join(', ')}}.

{% include 'shared/rapl_methodology.md' %}

{% include 'shared/statistical_methods.md' %}

[REF-2]
