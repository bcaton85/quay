/**
 * An element which displays a panel for managing users.
 */
angular.module('quay').directive('manageUserTab', function () {
  var directiveDefinitionObject = {
    priority: 0,
    templateUrl: '/static/directives/manage-users-tab.html',
    replace: false,
    transclude: true,
    restrict: 'C',
    scope: {
        'isEnabled': '=isEnabled'
    },
    controller: function ($scope, $timeout, $location, $element, ApiService, UserService,
                          TableService, Features, StateService) {
      $scope.inReadOnlyMode = StateService.inReadOnlyMode();
      $scope.Features = Features;
      UserService.updateUserIn($scope);
      $scope.users = null;
      $scope.orderedUsers = [];
      $scope.usersPerPage = 10;
      $scope.maxPage = 0;

      $scope.newUser = {};
      $scope.createdUser = null;
      $scope.takeOwnershipInfo = null;
      $scope.options = {
        'predicate': 'username',
        'reverse': false,
        'filter': null,
        'page': 0
      };
      $scope.disk_size_units = {
        'KB': 1024,
        'MB': 1024**2,
        'GB': 1024**3,
        'TB': 1024**4,
      };
      $scope.quotaUnits = Object.keys($scope.disk_size_units);
      $scope.loading = false;

      $scope.showQuotaConfig = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $('#quotaConfigModal-'+user.username).modal('show');
      };

      $scope.bytesToHumanReadableString = function(bytes) {
        let units = Object.keys($scope.disk_size_units).reverse();
        let result = null;
        let byte_unit = null;

        for (const key in units) {
          byte_unit = units[key];
          result = Math.round(bytes / $scope.disk_size_units[byte_unit]);
          if (bytes >= $scope.disk_size_units[byte_unit]) {
            return result.toString() + " " + byte_unit;
          }
        }

        return result.toString() + " " + byte_unit;
      };


      $scope.showCreateUser = function () {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $scope.createdUser = null;
        $('#createUserModal').modal('show');
      };

      var sortUsers = function() {
        if (!$scope.users) {return;}
        $scope.orderedUsers = TableService.buildOrderedItems($scope.users, $scope.options,
                                                             ['username', 'email'], []);
      };

      var loadUsersInternal = function (page = 0, nextPage = null) {
        var initialLoad = page == 0;
        var params = {}
        if(nextPage != null  && $scope.nextPage != ""){
          params["next_page"] = nextPage;
        }
        if($scope.options.filter != null && $scope.options.filter != "") {
          params["query"] = $scope.options.filter
        }
        if($scope.options.predicate != null  && $scope.options.predicate != "") {
          params["sort"] = $scope.options.predicate
        }
        if($scope.options.reverse) {
          params["direction"] = "desc"
        } else {
          params["direction"] = "asc"
        }
        $scope.loading = true;
        ApiService.listAllUsers(null, params).then(function (resp) {
          $scope.users = initialLoad ? resp['users'] : [...$scope.users, ...resp['users']];
          $scope.count = resp['count'];
          $scope.next_page_token = resp['next_page'];
          $scope.maxPage = initialLoad ? 0 : page;
          if(initialLoad){
            $scope.options.page = 0;
          }
          $scope.showInterface = true;
          $scope.loading = false;
        }, function (resp) {
          $scope.users = [];
          $scope.usersError = ApiService.getErrorMessage(resp);
          $scope.loading = false;
        });
      };

      $scope.tablePredicateClass = function(name, predicate, reverse) {
        if (name != predicate) {
          return '';
        }

        return 'current ' + (reverse ? 'reversed' : '');
      };

      $scope.orderBy = function(predicate) {
        if (predicate == $scope.options.predicate) {
          $scope.options.reverse = !$scope.options.reverse;
          return;
        }
        $scope.options.reverse = false;
        $scope.options.predicate = predicate;
      };

      $scope.createUser = function () {

        if (StateService.inReadOnlyMode()) {
          return;
        }

        $scope.creatingUser = true;
        $scope.createdUser = null;

        var errorHandler = ApiService.errorDisplay('Cannot create user', function () {
          $scope.creatingUser = false;
          $('#createUserModal').modal('hide');
        });

        ApiService.createInstallUser($scope.newUser, null).then(function (resp) {
          $scope.creatingUser = false;
          $scope.newUser = {};
          $scope.createdUser = resp;
          loadUsersInternal();
        }, errorHandler)
      };

      $scope.showChangeEmail = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $scope.userToChange = user;
        $('#changeEmailModal').modal({});
      };

      $scope.changeUserEmail = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $('#changeEmailModal').modal('hide');

        var params = {
          'username': user.username
        };

        var data = {
          'email': user.newemail
        };

        ApiService.changeInstallUser(data, params).then(function (resp) {
          loadUsersInternal();
          user.email = user.newemail;
          delete user.newemail;
        }, ApiService.errorDisplay('Could not change user'));
      };

      $scope.showChangePassword = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $scope.userToChange = user;
        $('#changePasswordModal').modal({});
      };

      $scope.changeUserPassword = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $('#changePasswordModal').modal('hide');

        var params = {
          'username': user.username
        };

        var data = {
          'password': user.password
        };

        ApiService.changeInstallUser(data, params).then(function (resp) {
          loadUsersInternal();
        }, ApiService.errorDisplay('Could not change user'));
      };

      $scope.sendRecoveryEmail = function (user) {
        var params = {
          'username': user.username
        };

        ApiService.sendInstallUserRecoveryEmail(null, params).then(function (resp) {
          bootbox.dialog({
            "message": "A recovery email has been sent to " + resp['email'],
            "title": "Recovery email sent",
            "buttons": {
              "close": {
                "label": "Close",
                "className": "btn-primary"
              }
            }
          });

        }, ApiService.errorDisplay('Cannot send recovery email'))
      };

      $scope.showDeleteUser = function (user) {
        if (user.username == UserService.currentUser().username) {
          bootbox.dialog({
            "message": 'Cannot delete yourself!',
            "title": "Cannot delete user",
            "buttons": {
              "close": {
                "label": "Close",
                "className": "btn-primary"
              }
            }
          });
          return;
        }

        $scope.userToDelete = user;
        $('#confirmDeleteUserModal').modal({});
      };

      $scope.deleteUser = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $('#confirmDeleteUserModal').modal('hide');

        var params = {
          'username': user.username
        };

        ApiService.deleteInstallUser(null, params).then(function (resp) {
          loadUsersInternal();
        }, ApiService.errorDisplay('Cannot delete user'));
      };

      $scope.askDisableUser = function (user) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        var message = 'Are you sure you want to disable this user? ' +
          'They will be unable to login, pull or push.';

        if (!user.enabled) {
          message = 'Are you sure you want to reenable this user? ' +
            'They will be able to login, pull or push.'
        }

        bootbox.confirm(message, function (resp) {
          if (resp) {
            var params = {
              'username': user.username
            };

            var data = {
              'enabled': !user.enabled
            };

            ApiService.changeInstallUser(data, params).then(function (resp) {
              loadUsersInternal();
            });
          }
        });
      };

      $scope.askTakeOwnership = function (entity) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        $scope.takeOwnershipInfo = {
          'entity': entity
        };
      };

      $scope.takeOwnership = function (info, callback) {
        if (StateService.inReadOnlyMode()) {
          return;
        }

        var errorDisplay = ApiService.errorDisplay('Could not take ownership of namespace', callback);
        var params = {
          'namespace': info.entity.username || info.entity.name
        };

        ApiService.takeOwnership(null, params).then(function () {
          callback(true);
          $location.path('/organization/' + params.namespace);
        }, errorDisplay)
      };

      $scope.$watch('isEnabled', function (value) {
        if (value) {
          if ($scope.users) {
            return;
          }
          loadUsersInternal();
        }
      });

      $scope.$watch('options.predicate', function(value){
        loadUsersInternal();
      });
      $scope.$watch('options.reverse', function(value){
        loadUsersInternal();
      });


      var debounce = null;
      $scope.$watch('options.filter', function(value){
        // Prevents request being made on each key input,
        // must wait 2.5 seconds with no input before making request
        if(debounce != null){
          clearTimeout(debounce);
        }
        debounce = setTimeout(loadUsersInternal,250)
      });
      $scope.$watch('options.page',function(value, oldValue){
        if(value > $scope.maxPage && !$scope.loading){
          loadUsersInternal(value, $scope.next_page_token);
        }
      })
    }
  };
  return directiveDefinitionObject;
});
