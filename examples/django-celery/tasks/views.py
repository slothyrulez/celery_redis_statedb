import datetime

from celery.result import AsyncResult
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from .tasks import (
    add,
    hello,
    multiply,
)


@require_http_methods(["GET"])
def trigger_add(request):
    """Trigger the add task."""
    x = int(request.GET.get("x", 10))
    y = int(request.GET.get("y", 20))

    task = add.delay(x, y)

    return JsonResponse(
        {
            "task_id": task.id,
            "status": "Task triggered",
            "x": x,
            "y": y,
        }
    )


@require_http_methods(["GET"])
def trigger_multiply(request):
    """Trigger the multiply task."""
    x = int(request.GET.get("x", 5))
    y = int(request.GET.get("y", 6))

    task = multiply.delay(x, y)

    return JsonResponse(
        {
            "task_id": task.id,
            "status": "Task triggered",
            "x": x,
            "y": y,
        }
    )


@require_http_methods(["GET"])
def trigger_hello(request):
    """Trigger the hello task."""
    task = hello.delay()

    return JsonResponse(
        {
            "task_id": task.id,
            "status": "Task triggered",
        }
    )


@require_http_methods(["GET"])
def trigger_future_task(request):
    """Trigger a future task."""
    eta = int(request.GET.get("eta", 10))
    datetime_eta = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=eta)

    task = hello.apply_async(eta=datetime_eta)

    return JsonResponse(
        {
            "task_id": task.id,
            "status": "Future task triggered",
            "eta": eta,
        }
    )


@require_http_methods(["GET"])
def task_status(request, task_id):
    """Get the status of a task."""
    task = AsyncResult(task_id)

    response = {
        "task_id": task_id,
        "state": task.state,
        "ready": task.ready(),
        "successful": task.successful() if task.ready() else None,
        "failed": task.failed() if task.ready() else None,
    }

    if task.ready():
        if task.successful():
            response["result"] = task.result
        elif task.failed():
            response["error"] = str(task.info)

    return JsonResponse(response)


@csrf_exempt
@require_http_methods(["POST"])
def revoke_task(request, task_id):
    """Revoke a task."""

    AsyncResult(task_id).revoke(terminate=True)

    return JsonResponse(
        {
            "task_id": task_id,
            "status": "Task revoked",
        }
    )


@require_http_methods(["GET"])
def list_tasks(request):
    """List all available tasks."""
    from myproject.celery import app

    tasks = list(app.tasks.keys())

    return JsonResponse(
        {
            "tasks": sorted(tasks),
        }
    )
