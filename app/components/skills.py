"""Skills section UI module for the LinkedIn resume Shiny application.

Provides Shiny module functions to display skills as tag badges
and a horizontal bar chart showing endorsement counts.
"""

import pandas as pd
from shiny import module, ui, render, reactive


def _build_skill_tag(skill_name: str) -> ui.Tag:
    """Build a single skill badge element.

    Args:
        skill_name: The name of the skill to display.

    Returns:
        An htmltools Tag representing a skill badge.
    """
    return ui.span(skill_name, class_="skill-tag")


def _build_skills_chart(df: pd.DataFrame) -> ui.Tag:
    """Build an SVG-based horizontal bar chart for skills with endorsements.

    Only skills that have a numeric endorsement count > 0 are included.
    Falls back to a tag-cloud view when no endorsement data is present.

    Args:
        df: A pandas DataFrame with columns: name, endorsements.

    Returns:
        An htmltools Tag containing either a bar chart or skill tags.
    """
    if "endorsements" not in df.columns:
        return None

    chart_df = df.copy()
    chart_df["endorsements"] = pd.to_numeric(
        chart_df["endorsements"], errors="coerce"
    ).fillna(0)
    chart_df = chart_df[chart_df["endorsements"] > 0].sort_values(
        "endorsements", ascending=True
    )

    if chart_df.empty:
        return None

    bar_height = 28
    gap = 8
    padding_left = 160
    padding_right = 40
    padding_top = 10
    n = len(chart_df)
    svg_height = n * (bar_height + gap) + padding_top * 2
    max_val = chart_df["endorsements"].max()
    chart_width = 480
    bar_area = chart_width - padding_left - padding_right

    bars = []
    for i, (_, row) in enumerate(chart_df.iterrows()):
        y = padding_top + i * (bar_height + gap)
        bar_w = int((row["endorsements"] / max_val) * bar_area) if max_val else 0
        bars.append(
            f'<text x="{padding_left - 8}" y="{y + bar_height // 2 + 5}" '
            f'text-anchor="end" class="skill-label">{row["name"]}</text>'
        )
        bars.append(
            f'<rect x="{padding_left}" y="{y}" width="{bar_w}" '
            f'height="{bar_height}" rx="4" class="skill-bar"/>'
        )
        bars.append(
            f'<text x="{padding_left + bar_w + 6}" y="{y + bar_height // 2 + 5}" '
            f'class="skill-count">{int(row["endorsements"])}</text>'
        )

    svg_content = (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{chart_width}" height="{svg_height}" '
        f'class="skills-chart">' + "".join(bars) + "</svg>"
    )

    return ui.HTML(svg_content)


@module.ui
def skills_ui() -> ui.Tag:
    """Shiny module UI for the skills section.

    Returns:
        A Shiny Tag containing the skills section placeholder.
    """
    return ui.div(
        ui.h2("Skills", class_="section-heading"),
        ui.output_ui("skills_chart_output"),
        ui.output_ui("skills_tags_output"),
        class_="section skills-section",
    )


@module.server
def skills_server(input, output, session, data: reactive.Value) -> None:
    """Shiny module server logic for the skills section.

    Args:
        input: Shiny input object (unused but required by module contract).
        output: Shiny output object.
        session: Shiny session object (unused but required by module contract).
        data: A reactive.Value containing a pandas DataFrame with skill
              records. Expected columns: name, endorsements (optional).
    """

    @output
    @render.ui
    def skills_chart_output() -> ui.Tag:
        """Render the endorsement bar chart if endorsement data exists."""
        df = data()
        if df is None or df.empty:
            return ui.span("")
        chart = _build_skills_chart(df)
        if chart is None:
            return ui.span("")
        return ui.div(chart, class_="skills-chart-wrapper")

    @output
    @render.ui
    def skills_tags_output() -> ui.Tag:
        """Render skill tags (badge cloud) from the provided data reactive."""
        df = data()
        if df is None or df.empty:
            return ui.p("No skills data available.", class_="no-data")
        tags = [_build_skill_tag(str(row.get("name", ""))) for _, row in df.iterrows()]
        return ui.div(*tags, class_="skills-tag-cloud")
