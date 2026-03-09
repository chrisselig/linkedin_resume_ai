"""Experience card UI module for the LinkedIn resume Shiny application.

Provides Shiny module functions to display job experience entries
as styled cards in a timeline layout.
"""

import pandas as pd
from shiny import module, ui, render, reactive


def _format_date_range(start: str, end: str) -> str:
    """Format a start/end date pair into a readable range string.

    Args:
        start: Start date string (e.g. "2020-01" or "Jan 2020").
        end: End date string or "Present".

    Returns:
        A formatted date range string like "Jan 2020 – Present".
    """
    end_display = end if end and str(end).strip() else "Present"
    return f"{start} – {end_display}"


def _build_experience_card(row: pd.Series) -> ui.Tag:
    """Build a single experience card HTML element from a DataFrame row.

    Args:
        row: A pandas Series representing one job experience record.
             Expected keys: title, company, location, start_date,
             end_date, description.

    Returns:
        An htmltools Tag representing the experience card.
    """
    date_range = _format_date_range(
        str(row.get("start_date", "")), str(row.get("end_date", ""))
    )
    description = row.get("description", "")
    location = row.get("location", "")

    return ui.div(
        ui.div(
            ui.h3(str(row.get("title", "")), class_="exp-title"),
            ui.p(
                ui.span(str(row.get("company", "")), class_="exp-company"),
                ui.span(" · ", class_="exp-sep") if location else ui.span(""),
                (
                    ui.span(str(location), class_="exp-location")
                    if location
                    else ui.span("")
                ),
                class_="exp-company-line",
            ),
            ui.p(date_range, class_="exp-dates"),
            (
                ui.p(str(description), class_="exp-description")
                if description
                else ui.span("")
            ),
            class_="exp-card-body",
        ),
        class_="exp-card",
    )


@module.ui
def experience_ui() -> ui.Tag:
    """Shiny module UI for the experience section.

    Returns:
        A Shiny Tag containing the experience section placeholder.
    """
    return ui.div(
        ui.h2("Experience", class_="section-heading"),
        ui.output_ui("experience_cards"),
        class_="section experience-section",
    )


@module.server
def experience_server(input, output, session, data: reactive.Value) -> None:
    """Shiny module server logic for the experience section.

    Args:
        input: Shiny input object (unused but required by module contract).
        output: Shiny output object.
        session: Shiny session object (unused but required by module contract).
        data: A reactive.Value containing a pandas DataFrame with experience
              records. Expected columns: title, company, location,
              start_date, end_date, description.
    """

    @output
    @render.ui
    def experience_cards() -> ui.Tag:
        """Render experience cards from the provided data reactive."""
        df = data()
        if df is None or df.empty:
            return ui.p("No experience data available.", class_="no-data")
        cards = [_build_experience_card(row) for _, row in df.iterrows()]
        return ui.div(*cards, class_="exp-timeline")
