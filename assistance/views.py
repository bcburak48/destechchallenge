from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .services import AssistanceService


class AssistanceRequestCreateView(APIView):
    def post(self, request):
        data = request.data
        try:
            assistance_req = AssistanceService.create_request(data)
            AssistanceService.assign_provider_atomic(assistance_req.id) 
            
            return Response({
                "status": "Created", 
                "id": assistance_req.id,
            }, status=status.HTTP_201_CREATED)
        except NotImplementedError:
            return Response(
                {"error": "Not implemented"},
                status=status.HTTP_501_NOT_IMPLEMENTED,
            )
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class AssistanceRequestCompleteView(APIView):
    def post(self, request_id):
        try:
            AssistanceService.complete_request(request_id)
            return Response({"status": "Completed"}, status=status.HTTP_200_OK)
        except NotImplementedError:
            return Response({"error": "Not implemented"}, status=status.HTTP_501_NOT_IMPLEMENTED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)


class AssistanceRequestCancelView(APIView):
    def post(self, request_id):
        try:
            AssistanceService.cancel_request(request_id)
            return Response({"status": "Cancelled"}, status=status.HTTP_200_OK)
        except NotImplementedError:
            return Response({"error": "Not implemented"}, status=status.HTTP_501_NOT_IMPLEMENTED)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_400_BAD_REQUEST)
