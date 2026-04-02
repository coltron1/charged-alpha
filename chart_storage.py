"""
Chart layout persistence — save/load TradingView chart state per user.
"""

from models import db, SavedChart


def save_chart_state(user_id, chart_name, symbol, state_json):
    """Save or update a chart layout for a user."""
    existing = SavedChart.query.filter_by(
        user_id=user_id, chart_name=chart_name
    ).first()

    if existing:
        existing.symbol = symbol
        existing.state_json = state_json
    else:
        chart = SavedChart(
            user_id=user_id,
            chart_name=chart_name,
            symbol=symbol,
            state_json=state_json,
        )
        db.session.add(chart)

    db.session.commit()
    return True


def load_chart_state(user_id, chart_name):
    """Load a saved chart layout. Returns dict or None."""
    chart = SavedChart.query.filter_by(
        user_id=user_id, chart_name=chart_name
    ).first()
    if not chart:
        return None
    return {
        "id": chart.id,
        "chart_name": chart.chart_name,
        "symbol": chart.symbol,
        "state_json": chart.state_json,
        "updated_at": chart.updated_at.isoformat() if chart.updated_at else None,
    }


def list_user_charts(user_id):
    """List all saved charts for a user."""
    charts = SavedChart.query.filter_by(user_id=user_id)\
        .order_by(SavedChart.updated_at.desc()).all()
    return [{
        "id": c.id,
        "chart_name": c.chart_name,
        "symbol": c.symbol,
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    } for c in charts]


def delete_chart_state(user_id, chart_name):
    """Delete a saved chart layout."""
    chart = SavedChart.query.filter_by(
        user_id=user_id, chart_name=chart_name
    ).first()
    if chart:
        db.session.delete(chart)
        db.session.commit()
        return True
    return False
