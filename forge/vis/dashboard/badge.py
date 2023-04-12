import typing
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from forge.vis.util import package_template
from forge.dashboard.database import Severity
from .entry import Entry


def assemble_badge_json(entry: Entry) -> Response:
    return JSONResponse({'status': entry.status.value})


async def assemble_badge_svg(request: Request, entry: Entry,
                             template_name: typing.Optional[str] = None, **kwargs) -> Response:
    if not template_name:
        template_name = 'basic.svg'
    label = request.query_params.get('label')
    if label is not None:
        label = str(label)[:255]
    else:
        label = entry.display

    status = request.query_params.get(entry.status.value)
    if status is not None:
        status = str(status)[:255]
    else:
        status = entry.status.name

    return Response((await package_template('dashboard', 'badge', template_name).render_async(
        request=request,
        entry=entry,
        label=label,
        status=status,
        Severity=Severity,
        ord=ord,
        **kwargs
    )).strip(), media_type='image/svg+xml')
